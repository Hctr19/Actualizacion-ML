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
SHEET_NAME = 'PUBLICACIONES'
CONFIG_SHEET = 'Config_ML'
HISTORY_SHEET = 'Historial'
HORAS_ATRAS = 48 

# --- CONFIGURACIÓN DISCORD ---
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

def clean_float(value):
    """Limpia valores de la hoja para evitar errores de conversión."""
    if value is None: return 0.0
    s_val = str(value).strip().replace(',', '')
    if s_val == '' or '#N/A' in s_val.upper() or 'NONE' in s_val.upper():
        return 0.0
    try:
        return float(s_val)
    except ValueError:
        return 0.0

def clean_int(value):
    """Limpia valores enteros de la hoja."""
    return int(clean_float(value))

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
        print(f"❌ Error Refresh Token: {res.status_code} - {res.text}")
        return None
    except Exception as e:
        print(f"❌ Error crítico en get_new_token: {e}")
        return None

def get_data(i_id, token):
    """Obtiene detalles, precio y comisiones exactas con validaciones."""
    headers = {'Authorization': f'Bearer {token}'}
    try:
        # 1. Datos básicos del item
        res_it = requests.get(f"https://api.mercadolibre.com/items/{i_id}", headers=headers, timeout=15)
        if res_it.status_code != 200:
            print(f"⚠️ Error Item {i_id}: {res_it.status_code}")
            return None
        it = res_it.json()

        # 2. Precio de venta (Promos)
        res_sp = requests.get(f"https://api.mercadolibre.com/items/{i_id}/sale_price", headers=headers, timeout=15)
        sp = res_sp.json() if res_sp.status_code == 200 else {}
        
        # Determinamos precio actual (promo o base)
        p_promo = sp.get('amount') or it.get('price')
        if p_promo is None:
            print(f"⚠️ Item {i_id} no tiene precio definido.")
            return None

        # 3. Comisión (sale_fee)
        sf_url = f"https://api.mercadolibre.com/items/{i_id}/sale_fee?price={p_promo}"
        res_sf = requests.get(sf_url, headers=headers, timeout=15)
        comision = 0.0
        if res_sf.status_code == 200:
            comision = res_sf.json().get('sale_fee_amount', 0.0)
        else:
            print(f"⚠️ No se pudo obtener comisión para {i_id} (Status: {res_sf.status_code})")
            # Fallback sin parámetro de precio
            res_sf_fallback = requests.get(f"https://api.mercadolibre.com/items/{i_id}/sale_fee", headers=headers, timeout=15)
            if res_sf_fallback.status_code == 200:
                comision = res_sf_fallback.json().get('sale_fee_amount', 0.0)

        return {'body': it, 'promo_price': p_promo, 'comision': comision}
    except Exception as e:
        print(f"❌ Error en request para {i_id}: {e}")
        return None

def run_update():
    print(f"🚀 Iniciando sincronización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # --- AUTENTICACIÓN ---
    try:
        sa_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
        gc = gspread.service_account_from_dict(sa_info)
        sh = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        print(f"❌ Error Auth Google: {e}")
        return
    
    access_token = get_new_token(sh.worksheet(CONFIG_SHEET))
    if not access_token:
        print("🛑 Abortando: No hay token de acceso válido.")
        return

    # --- OBTENER DATOS DE LA HOJA ---
    worksheet = sh.worksheet(SHEET_NAME)
    records = worksheet.get_all_records()
    if not records:
        print("⚠️ La hoja está vacía.")
        return
        
    df = pd.DataFrame(records).fillna('')
    unique_ids = [str(x).strip() for x in df['Item ID'].unique() if x != '']
    print(f"📦 Procesando {len(unique_ids)} publicaciones únicas...")

    # --- DESCARGA CONCURRENTE ---
    item_details = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        f_to_id = {executor.submit(get_data, i_id, access_token): i_id for i_id in unique_ids}
        for f in concurrent.futures.as_completed(f_to_id):
            res = f.result()
            if res:
                item_details[f_to_id[f]] = res

    log_reporte = []
    alertas_para_discord = [] 
    limite_historial = datetime.now(pytz.utc) - timedelta(hours=HORAS_ATRAS)
    hubo_cambios = False

    # --- PROCESAR FILAS ---
    for i, row in df.iterrows():
        it_id = str(row['Item ID']).strip()
        if it_id in item_details:
            data = item_details[it_id]
            item = data['body']
            v_id = str(row['Variant ID']).strip()

            # Lógica de Logística
            logistica_real = item.get('shipping', {}).get('logistic_type', 'not_specified')
            sheet_logistica = str(row.get('Logistica', '')).strip()

            # Stock real (Variante o Item)
            if v_id and v_id not in ['0', '', 'None']:
                v_data = next((v for v in item.get('variations', []) if str(v.get('id')) == v_id), None)
                stock_real = v_data.get('available_quantity', 0) if v_data else 0
            else:
                stock_real = item.get('available_quantity', 0)

            es_full = (logistica_real == 'fulfillment')
            
            # Estatus
            api_status = item.get('status') 
            nuevo_estatus = "Activa" if api_status == 'active' and stock_real > 0 else "Pausada"
            sheet_estatus = str(row['Estatus']).strip().capitalize()

            # Detección de cambios con protección
            nuevo_p_promo = float(data['promo_price'])
            nuevo_p_base = float(item.get('original_price') or item.get('price') or nuevo_p_promo)
            nuevo_stock = int(stock_real) if es_full else 0
            nueva_comision = float(data['comision'])
            
            sheet_promo = clean_float(row.get('Precio Promo'))
            sheet_stock = clean_int(row.get('Stock (Solo Full)'))
            sheet_comision = clean_float(row.get('Comision'))

            # Solo marcar cambio si el nuevo valor NO es cero (protección)
            cambio_en_fila = (
                (sheet_estatus != nuevo_estatus) or 
                (abs(sheet_promo - nuevo_p_promo) > 0.01 and nuevo_p_promo > 0) or 
                (sheet_stock != nuevo_stock) or
                (sheet_logistica != logistica_real) or
                (abs(sheet_comision - nueva_comision) > 0.01 and nueva_comision > 0)
            )

            if cambio_en_fila:
                df.at[i, 'Precio Base'] = nuevo_p_base
                df.at[i, 'Precio Promo'] = nuevo_p_promo
                df.at[i, 'Stock (Solo Full)'] = nuevo_stock
                df.at[i, 'Estatus'] = nuevo_estatus
                df.at[i, 'Logistica'] = logistica_real
                df.at[i, 'Comision'] = nueva_comision
                hubo_cambios = True

                # Alertas Discord si es pausa inesperada
                if sheet_estatus == "Activa" and nuevo_estatus == "Pausada":
                    titulo = item.get('title', 'Producto')[:25]
                    alertas_para_discord.append(f"• {it_id} | {titulo} | Pausado")

                # Historial
                try:
                    fecha_mod_ml = datetime.fromisoformat(item.get('last_updated', '').replace('Z', '+00:00'))
                    if fecha_mod_ml > limite_historial:
                        log_reporte.append([it_id, "Actualización de datos", fecha_mod_ml.strftime("%d/%m/%Y %H:%M")])
                except: continue

    # --- GUARDAR SI HUBO CAMBIOS ---
    if hubo_cambios:
        print("📝 Guardando cambios en Google Sheets...")
        try:
            worksheet.update([df.columns.values.tolist()] + df.astype(str).values.tolist(), 'A1')
            
            h_ws = next((w for w in sh.worksheets() if w.title.strip() == HISTORY_SHEET), None)
            if h_ws and log_reporte:
                h_ws.append_rows(log_reporte)
        except Exception as e:
            print(f"❌ Error al guardar en Sheets: {e}")

    # --- DISCORD ---
    if alertas_para_discord and DISCORD_WEBHOOK_URL:
        mensaje = "⚠️ **CAMBIOS DETECTADOS** ⚠️\n
