import pandas as pd
import requests
import concurrent.futures
import gspread
import os
import json
from datetime import datetime, timedelta
import pytz

# --- CONFIGURACIÓN UNIFICADA ---
SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
SHEET_NAME = 'ML'
CONFIG_SHEET = 'Config_ML'
HISTORY_SHEET = 'Historial'

# Si quieres 2 días, ponemos 48 horas. 
# Esto afectará tanto a qué se anota como a qué se borra del historial.
HORAS_ATRAS = 48 

# --- CONFIGURACIÓN TELEGRAM ---
TELEGRAM_TOKEN = '8630840503:AAHKJjGfE7xrW67CmGpJ5S-lua8 68nGfaRg'
TELEGRAM_CHAT_ID = '7421757172'

def get_new_token(config_ws):
    try:
        c_id = str(config_ws.acell('A2').value).replace(',', '').replace(' ', '').strip()
        c_secret = str(config_ws.acell('B2').value).strip()
        r_token = str(config_ws.acell('C2').value).strip()
        url = "https://api.mercadolibre.com/oauth/token"
        res = requests.post(url, data={'grant_type': 'refresh_token', 'client_id': c_id, 'client_secret': c_secret, 'refresh_token': r_token})
        if res.status_code == 200:
            token_data = res.json()
            config_ws.update_acell('C2', token_data['refresh_token'])
            return token_data['access_token']
        return None
    except: return None

def get_data(i_id, token):
    headers = {'Authorization': f'Bearer {token}'}
    try:
        it = requests.get(f"https://api.mercadolibre.com/items/{i_id}", headers=headers, timeout=15).json()
        sp = requests.get(f"https://api.mercadolibre.com/items/{i_id}/sale_price", headers=headers, timeout=15).json()
        p_promo = sp.get('amount') or it.get('price') or 0
        return {'body': it, 'promo_price': p_promo}
    except: return None

def enviar_alerta_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': mensaje, 'parse_mode': 'Markdown'}
    try:
        res = requests.post(url, data=payload, timeout=10)
        if res.status_code != 200:
            print(f"❌ Error de Telegram: {res.text}") # Esto te dirá por qué falló
        else:
            print("✅ Mensaje enviado a Telegram correctamente")
    except Exception as e:
        print(f"❌ Error de conexión con Telegram: {e}")

def actualizar_historial_limpio(h_ws, nuevos_logs):
    """Limpia registros antiguos basados en HORAS_ATRAS y ordena."""
    datos_actuales = h_ws.get_all_values()
    encabezado = ["Item ID", "Cual fue el cambio", "Ultima Modificacion ML"]
    registros_validos = []
    
    # El límite de borrado es el mismo que el de detección
    limite_tiempo = datetime.now() - timedelta(hours=HORAS_ATRAS)
    formato_fecha = "%d/%m/%Y %H:%M"

    if len(datos_actuales) > 1:
        for fila in datos_actuales[1:]:
            try:
                if len(fila) >= 3 and fila[2]:
                    fecha_reg = datetime.strptime(fila[2], formato_fecha)
                    if fecha_reg >= limite_tiempo:
                        registros_validos.append(fila)
            except: continue

    todos = registros_validos + nuevos_logs
    try:
        todos.sort(key=lambda x: datetime.strptime(x[2], formato_fecha), reverse=True)
    except: pass

    h_ws.clear()
    h_ws.update([encabezado] + todos, 'A1')

def run_update():
    sa_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
    gc = gspread.service_account_from_dict(sa_info)
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    access_token = get_new_token(sh.worksheet(CONFIG_SHEET))
    if not access_token: return

    worksheet = sh.worksheet(SHEET_NAME)
    df = pd.DataFrame(worksheet.get_all_records()).fillna('')
    unique_ids = df['Item ID'].unique().tolist()

    item_details = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        f_to_id = {executor.submit(get_data, i_id, access_token): i_id for i_id in unique_ids}
        for f in concurrent.futures.as_completed(f_to_id):
            res = f.result()
            if res: item_details[f_to_id[f]] = res

    log_reporte = []
    # Límite para decidir si anotamos en el historial
    limite_historial = datetime.now(pytz.utc) - timedelta(hours=HORAS_ATRAS)
    hubo_cambios = False

    for i, row in df.iterrows():
        it_id = str(row['Item ID']).strip()
        if it_id in item_details:
            data = item_details[it_id]
            item = data['body']
            v_id = str(row['Variant ID']).strip()

            # Stock real
            if v_id and v_id not in ['0', '', 'None']:
                v_data = next((v for v in item.get('variations', []) if str(v.get('id')) == v_id), None)
                stock_real = v_data.get('available_quantity', 0) if v_data else 0
            else:
                stock_real = item.get('available_quantity', 0)

            es_full = item.get('shipping', {}).get('logistic_type') == 'fulfillment'
            nuevo_estatus = "Activa" if item.get('status') == 'active' and stock_real > 0 else "Pausada"
            nuevo_p_promo = float(data.get('promo_price') or 0.0)
            nuevo_stock = int(stock_real) if es_full else 0
            nuevo_p_base = float(item.get('original_price') or item.get('price') or 0.0)
            
            sheet_estatus = str(row['Estatus']).strip()
            sheet_promo = float(row['Precio Promo']) if row['Precio Promo'] != '' else 0.0
            sheet_stock = int(row['Stock (Solo Full)']) if row['Stock (Solo Full)'] != '' else 0

           # --- NUEVA LÓGICA DE ALERTA: NOTIFICAR CUALQUIER PAUSA EN FULL ---
            # Si el producto es de Full y antes estaba Activo, pero ahora está Pausado
            if es_full and sheet_estatus == "Activa" and nuevo_estatus == "Pausada":
                razon_pausa = "Sin Stock" if nuevo_stock == 0 else "Motivo Externo (Manual/ML)"
                
                mensaje = (f"🚫 *PUBLICACIÓN PAUSADA EN FULL*\n\n"
                           f"📦 *Producto:* {item.get('title')[:60]}...\n"
                           f"🆔 *ID:* `{it_id}`\n"
                           f"❓ *Posible causa:* {razon_pausa}\n"
                           f"🔗 [Ver en ML]({item.get('permalink')})")
                
                enviar_alerta_telegram(mensaje)

            # --- DETECCIÓN DE CAMBIOS ---
            if (sheet_estatus != nuevo_estatus) or (abs(sheet_promo - nuevo_p_promo) > 0.01) or (sheet_stock != nuevo_stock):
                df.at[i, 'Precio Base'] = nuevo_p_base
                df.at[i, 'Precio Promo'] = nuevo_p_promo
                df.at[i, 'Stock (Solo Full)'] = nuevo_stock
                df.at[i, 'Estatus'] = nuevo_estatus
                hubo_cambios = True

                # Anotar en log si el cambio en ML es reciente
                last_up_str = item.get('last_updated', '').replace('Z', '+00:00')
                try:
                    fecha_mod_ml = datetime.fromisoformat(last_up_str)
                    if fecha_mod_ml > limite_historial:
                        cambios = []
                        if sheet_estatus != nuevo_estatus: cambios.append(f"Stat: {sheet_estatus}->{nuevo_estatus}")
                        if sheet_stock != nuevo_stock: cambios.append(f"Stock: {sheet_stock}->{nuevo_stock}")
                        if abs(sheet_promo - nuevo_p_promo) > 0.01: cambios.append(f"P: {sheet_promo}->{nuevo_p_promo}")
                        
                        log_reporte.append([it_id, " | ".join(cambios), fecha_mod_ml.strftime("%d/%m/%Y %H:%M")])
                except: continue

    if hubo_cambios:
        worksheet.update([df.columns.values.tolist()] + df.astype(str).values.tolist(), 'A1')
        if log_reporte:
            h_ws = next((w for w in sh.worksheets() if w.title.strip() == HISTORY_SHEET), None)
            if h_ws is None:
                h_ws = sh.add_worksheet(title=HISTORY_SHEET, rows="5000", cols="3")
                h_ws.append_row(["Item ID", "Cual fue el cambio", "Ultima Modificacion ML"])
            actualizar_historial_limpio(h_ws, log_reporte)

if __name__ == "__main__":
    run_update()
