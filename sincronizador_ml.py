import pandas as pd
import requests
import concurrent.futures
import gspread
import os
import json
from datetime import datetime, timedelta
import pytz

# --- CONFIGURACIÓN PRINCIPAL ---
SPREADSHEET_ID = os.environ['SPREADSHEET_ID'] 
SHEET_NAME = 'ML'
CONFIG_SHEET = 'Config_ML'
HISTORY_SHEET = 'Historial'

# Mantenemos las 48 horas para detectar cambios
HORAS_ATRAS = 48 

# --- CONFIGURACIÓN DISCORD ---
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

def get_new_token(config_ws):
    """Refresca el token de acceso de Mercado Libre."""
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
    """Obtiene detalles del item y precio promocional."""
    headers = {'Authorization': f'Bearer {token}'}
    try:
        it = requests.get(f"https://api.mercadolibre.com/items/{i_id}", headers=headers, timeout=15).json()
        sp = requests.get(f"https://api.mercadolibre.com/items/{i_id}/sale_price", headers=headers, timeout=15).json()
        p_promo = sp.get('amount') or it.get('price') or 0
        return {'body': it, 'promo_price': p_promo}
    except: return None

def enviar_alerta_discord(mensaje):
    """Envía un mensaje al webhook de Discord configurado."""
    payload = {"content": mensaje}
    try:
        res = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if res.status_code != 204:
            print(f"❌ Error Discord: {res.status_code} - {res.text}")
    except:
        print("❌ Fallo de conexión con Discord")

def actualizar_historial_limpio(h_ws, nuevos_logs):
    """Filtra y actualiza la hoja de historial manteniendo el orden cronológico."""
    datos_actuales = h_ws.get_all_values()
    encabezado = ["Item ID", "Cual fue el cambio", "Ultima Modificacion ML"]
    registros_validos = []
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

    todos_limitados = todos[:5000]
    h_ws.clear()
    h_ws.update([encabezado] + todos_limitados, 'A1')

def run_update():
    # --- AUTENTICACIÓN GOOGLE ---
    sa_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
    gc = gspread.service_account_from_dict(sa_info)
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # --- REFRESH TOKEN ML ---
    access_token = get_new_token(sh.worksheet(CONFIG_SHEET))
    if not access_token: 
        print("Error: No se pudo refrescar el token de ML")
        return

    # --- OBTENER DATOS DE LA HOJA 'ML' ---
    worksheet = sh.worksheet(SHEET_NAME)
    df = pd.DataFrame(worksheet.get_all_records()).fillna('')
    unique_ids = df['Item ID'].unique().tolist()

    # --- DESCARGA CONCURRENTE (25 workers) ---
    item_details = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        f_to_id = {executor.submit(get_data, i_id, access_token): i_id for i_id in unique_ids}
        for f in concurrent.futures.as_completed(f_to_id):
            res = f.result()
            if res: item_details[f_to_id[f]] = res

    log_reporte = []
    alertas_para_discord = [] 
    limite_historial = datetime.now(pytz.utc) - timedelta(hours=HORAS_ATRAS)
    hubo_cambios = False

    # --- PROCESAR CAMBIOS ---
    for i, row in df.iterrows():
        it_id = str(row['Item ID']).strip()
        if it_id in item_details:
            data = item_details[it_id]
            item = data['body']
            v_id = str(row['Variant ID']).strip()

            # --- NUEVO: OBTENER LOGÍSTICA ---
            logistica_real = item.get('shipping', {}).get('logistic_type', 'not_specified')
            sheet_logistica = str(row.get('Logistica', '')).strip()

            # Stock real
            if v_id and v_id not in ['0', '', 'None']:
                v_data = next((v for v in item.get('variations', []) if str(v.get('id')) == v_id), None)
                stock_real = v_data.get('available_quantity', 0) if v_data else 0
            else:
                stock_real = item.get('available_quantity', 0)

            es_full = logistica_real == 'fulfillment'
            
            # --- TRADUCTOR ESTATUS ---
            api_status = item.get('status') 
            nuevo_estatus = "Activa" if api_status == 'active' and stock_real > 0 else "Pausada"
            sheet_estatus = str(row['Estatus']).strip().capitalize() 

            # --- SUB-STATUS Y MODERACIONES ---
            sub_status_list = item.get('sub_status', [])
            sub_status_str = ", ".join(sub_status_list) if sub_status_list else "N/A"
            
            if sheet_estatus == "Activa":
                es_revision = "under_review" in sub_status_list or "waiting_for_patch" in sub_status_list
                if nuevo_estatus == "Pausada" or es_revision:
                    if es_revision:
                        razon = f"⚠️ BAJO REVISIÓN ({sub_status_str})"
                    elif es_full and stock_real == 0:
                        razon = "Pausada (Sin Stock en Full)"
                    else:
                        razon = f"Pausada (Sub: {sub_status_str})"

                    titulo_caja = item.get('title', 'Producto')[:30]
                    alertas_para_discord.append(f"• {it_id} | {titulo_caja:<30} | {razon}")
                    
            # --- DETECCIÓN DE CAMBIOS ---
            nuevo_p_promo = float(data.get('promo_price') or 0.0)
            nuevo_stock = int(stock_real) if es_full else 0
            nuevo_p_base = float(item.get('original_price') or item.get('price') or 0.0)
            
            sheet_promo = float(row['Precio Promo']) if row['Precio Promo'] != '' else 0.0
            sheet_stock = int(row['Stock (Solo Full)']) if row['Stock (Solo Full)'] != '' else 0

            # Verificamos si cambió algo, incluyendo la logística
            cambio_en_fila = (
                (sheet_estatus != nuevo_estatus) or 
                (abs(sheet_promo - nuevo_p_promo) > 0.01) or 
                (sheet_stock != nuevo_stock) or
                (sheet_logistica != logistica_real)
            )

            if cambio_en_fila:
                df.at[i, 'Precio Base'] = nuevo_p_base
                df.at[i, 'Precio Promo'] = nuevo_p_promo
                df.at[i, 'Stock (Solo Full)'] = nuevo_stock
                df.at[i, 'Estatus'] = nuevo_estatus
                df.at[i, 'Logistica'] = logistica_real # Actualiza la columna H
                hubo_cambios = True

                last_up_str = item.get('last_updated', '').replace('Z', '+00:00')
                try:
                    fecha_mod_ml = datetime.fromisoformat(last_up_str)
                    if fecha_mod_ml > limite_historial:
                        cambios = []
                        if sheet_estatus != nuevo_estatus: cambios.append(f"Stat: {sheet_estatus}->{nuevo_estatus}")
                        if sheet_stock != nuevo_stock: cambios.append(f"Stock: {sheet_stock}->{nuevo_stock}")
                        if sheet_logistica != logistica_real: cambios.append(f"Log: {sheet_logistica}->{logistica_real}")
                        if abs(sheet_promo - nuevo_p_promo) > 0.01: cambios.append(f"P: {sheet_promo}->{nuevo_p_promo}")
                        
                        log_reporte.append([it_id, " | ".join(cambios), fecha_mod_ml.strftime("%d/%m/%Y %H:%M")])
                except: continue

    # --- ENVIAR RESUMEN A DISCORD ---
    if alertas_para_discord:
        chunks_alertas = [alertas_para_discord[x:x+15] for x in range(0, len(alertas_para_discord), 15)]
        for grupo in chunks_alertas:
            texto_alertas = "\n".join(grupo)
            mensaje_final = (
                f"⚠️ **RESUMEN DE PAUSAS EN FULL ({datetime.now().strftime('%H:%M')})** ⚠️\n"
                f"```yaml\n"
                f"{texto_alertas}\n"
                f"```"
            )
            enviar_alerta_discord(mensaje_final)

    # --- GUARDAR RESULTADOS EN GOOGLE SHEETS ---
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
