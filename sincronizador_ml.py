import pandas as pd
import requests
import concurrent.futures
import gspread
import os
import json
from datetime import datetime, timedelta
import pytz

# --- CONFIGURACIÓN ---
SPREADSHEET_ID = os.environ['SPREADSHEET_ID']
SHEET_NAME = 'ML'
CONFIG_SHEET = 'Config_ML'
HISTORY_SHEET = 'Historial'
HORAS_ATRAS = 2 # Filtro de recencia

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
        return {'body': it, 'promo_price': sp.get('amount', it.get('price'))}
    except: return None

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
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        f_to_id = {executor.submit(get_data, i_id, access_token): i_id for i_id in unique_ids}
        for f in concurrent.futures.as_completed(f_to_id):
            res = f.result()
            if res: item_details[f_to_id[f]] = res

    log_reporte = []
    limite = datetime.now(pytz.utc) - timedelta(hours=HORAS_ATRAS)
    hubo_cambios = False

    for i, row in df.iterrows():
        it_id = str(row['Item ID']).strip()
        if it_id in item_details:
            data = item_details[it_id]
            item = data['body']
            v_id = str(row['Variant ID']).strip()

            # Lógica de variantes y stock
            if v_id and v_id not in ['0', '', 'None']:
                v_data = next((v for v in item.get('variations', []) if str(v.get('id')) == v_id), None)
                stock_real = v_data.get('available_quantity', 0) if v_data else 0
            else:
                stock_real = item.get('available_quantity', 0)

            # --- VALORES NUEVOS DE ML ---
            nuevo_estatus = "Activa" if item.get('status') == 'active' and stock_real > 0 else "Pausada"
            nuevo_p_promo = float(data['promo_price'])
            nuevo_stock = int(stock_real) if item.get('shipping', {}).get('logistic_type') == 'fulfillment' else 0
            nuevo_p_base = float(item.get('original_price') or item.get('price'))
            
            # --- VALORES ACTUALES EN SHEET ---
            sheet_estatus = str(row['Estatus']).strip()
            sheet_promo = float(row['Precio Promo']) if row['Precio Promo'] != '' else 0.0
            sheet_stock = int(row['Stock (Solo Full)']) if row['Stock (Solo Full)'] != '' else 0

            # --- DETECCIÓN DE CAMBIOS ---
            cambios_fila = []
            if sheet_estatus != nuevo_estatus:
                cambios_fila.append(f"Status: {sheet_estatus}->{nuevo_estatus}")
            if abs(sheet_promo - nuevo_p_promo) > 0.01:
                cambios_fila.append(f"Promo: {sheet_promo}->{nuevo_p_promo}")
            if sheet_stock != nuevo_stock:
                cambios_fila.append(f"Stock: {sheet_stock}->{nuevo_stock}")

            if cambios_fila:
                # Actualizar DataFrame principal
                df.at[i, 'Precio Base'] = nuevo_p_base
                df.at[i, 'Precio Promo'] = nuevo_p_promo
                df.at[i, 'Stock (Solo Full)'] = nuevo_stock
                df.at[i, 'Estatus'] = nuevo_estatus
                hubo_cambios = True

                # Registrar en Historial solo si el cambio en ML es reciente
                last_up_str = item.get('last_updated', '').replace('Z', '+00:00')
                fecha_mod_ml = datetime.fromisoformat(last_up_str)
                
                if fecha_mod_ml > limite:
                    log_reporte.append([
                        it_id, 
                        " | ".join(cambios_fila), 
                        fecha_mod_ml.strftime("%d/%m/%Y %H:%M")
                    ])

    # --- GUARDADO ---
    if hubo_cambios:
        worksheet.update([df.columns.values.tolist()] + df.astype(str).values.tolist(), 'A1')
        
        if log_reporte:
            todas_las_hojas = sh.worksheets()
            h_ws = next((w for w in todas_las_hojas if w.title.strip() == HISTORY_SHEET), None)
            if h_ws is None:
                h_ws = sh.add_worksheet(title=HISTORY_SHEET, rows="5000", cols="3")
                h_ws.append_row(["Item ID", "Cual fue el cambio", "Ultima Modificacion ML"])
            
            h_ws.append_rows(log_reporte)

if __name__ == "__main__":
    run_update()
