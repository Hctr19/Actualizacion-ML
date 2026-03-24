import pandas as pd
import requests
import concurrent.futures
import gspread
import os
import json
from datetime import datetime, timedelta
import pytz

# --- CONFIGURACIÓN DESDE SECRETOS DE GITHUB ---
# Usamos variables de entorno para no exponer tus llaves en el código
SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
SHEET_NAME = 'ML'
CONFIG_SHEET = 'Config_ML'
HISTORY_SHEET = 'Historial'
HORAS_ATRAS = 2 

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
    except: return None

def run_update():
    # Autenticación con Service Account (vía Secreto de GitHub)
    service_account_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
    gc = gspread.service_account_from_dict(service_account_info)
    
    sh = gc.open_by_key(SPREADSHEET_ID)
    access_token = get_new_token(sh.worksheet(CONFIG_SHEET))
    if not access_token: return

    # Carga de datos
    worksheet = sh.worksheet(SHEET_NAME)
    df = pd.DataFrame(worksheet.get_all_records()).fillna('')
    unique_ids = df['Item ID'].unique().tolist()

    # Consulta API (Simplificada para logs de GitHub)
    def get_data(i_id):
        headers = {'Authorization': f'Bearer {access_token}'}
        try:
            it = requests.get(f"https://api.mercadolibre.com/items/{i_id}", headers=headers, timeout=10).json()
            sp = requests.get(f"https://api.mercadolibre.com/items/{i_id}/sale_price", headers=headers, timeout=10).json()
            return {'body': it, 'promo_price': sp.get('amount', it.get('price'))}
        except: return None

    item_details = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_id = {executor.submit(get_data, i_id): i_id for i_id in unique_ids}
        for future in concurrent.futures.as_completed(future_to_id):
            res = future.result()
            if res: item_details[future_to_id[future]] = res

    # Lógica de detección y registro
    log_reporte = []
    limite = datetime.now(pytz.utc) - timedelta(hours=HORAS_ATRAS)

    for i, row in df.iterrows():
        it_id = str(row['Item ID']).strip()
        if it_id in item_details:
            item = item_details[it_id]['body']
            # ... (Aquí va tu lógica original de stock/variantes que ya tenemos) ...
            # Para este ejemplo, simplificamos el cálculo del cambio:
            last_up = datetime.fromisoformat(item.get('last_updated', '').replace('Z', '+00:00'))
            
            # Si el cambio en ML es reciente, lo guardamos para el Historial
            if last_up > limite:
                log_reporte.append([
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                    it_id,
                    f"Cambio detectado en ML (Last Update: {last_up.strftime('%H:%M')})"
                ])

    # 1. Actualizar Sheet Principal
    worksheet.update([df.columns.values.tolist()] + df.astype(str).values.tolist(), 'A1')

    # 2. Agregar al Historial (si hay cambios)
    if log_reporte:
        try:
            hist_ws = sh.worksheet(HISTORY_SHEET)
        except:
            hist_ws = sh.add_worksheet(title=HISTORY_SHEET, rows="100", cols="5")
            hist_ws.append_row(["Fecha Ejecución", "Item ID", "Detalle"])
        
        hist_ws.append_rows(log_reporte)
        print(f"✅ {len(log_reporte)} cambios añadidos al historial.")

if __name__ == "__main__":
    run_update()
