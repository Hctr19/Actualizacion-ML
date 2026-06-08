import pandas as pd
import requests
import concurrent.futures
import gspread
import os
import json
from datetime import datetime
import sys
from dotenv import load_dotenv

# Configurar la consola para soportar caracteres UTF-8 en Windows
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# Cargar variables de entorno desde el archivo .env o uno especificado por argumento --env si no están ya en el sistema
if 'SPREADSHEET_ID' not in os.environ:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_file = os.path.join(script_dir, ".env")
    if "--env" in sys.argv:
        try:
            idx = sys.argv.index("--env")
            if idx + 1 < len(sys.argv):
                env_file_arg = sys.argv[idx + 1]
                if not os.path.isabs(env_file_arg):
                    env_file = os.path.join(script_dir, env_file_arg)
                else:
                    env_file = env_file_arg
                # Eliminar los argumentos del entorno para no interferir
                sys.argv.pop(idx + 1)
                sys.argv.pop(idx)
        except ValueError:
            pass

    print(f"ℹ️ [DEBUG] Cargando variables desde: {env_file}")
    load_dotenv(env_file)

# Importar configuraciones y helpers desde el sincronizador principal
from sincronizador_ml import (
    get_new_token, get_data, SPREADSHEET_ID, SHEET_NAME, CONFIG_SHEET, obtener_sku
)

def obtener_user_id(token):
    """Obtiene el ID numérico del vendedor autenticado."""
    headers = {'Authorization': f'Bearer {token}'}
    try:
        res = requests.get("https://api.mercadolibre.com/users/me", headers=headers, timeout=15)
        if res.status_code == 200:
            user_id = res.json().get('id')
            print(f"✅ [DEBUG] User ID obtenido con éxito: {user_id}")
            return user_id
        else:
            print(f"❌ Error al obtener User ID: {res.status_code} - {res.text}")
            return None
    except Exception as e:
        print(f"❌ Error de conexión al obtener User ID: {str(e)}")
        return None

def obtener_todos_items_api(user_id, token):
    """Obtiene todos los Item IDs de Mercado Libre del vendedor usando paginación modo scan."""
    headers = {'Authorization': f'Bearer {token}'}
    item_ids = []
    scroll_id = None
    limit = 100
    
    print("🔍 [DEBUG] Consultando catálogo completo de publicaciones en la API (Modo Scan)...")
    while True:
        if scroll_id is None:
            url = f"https://api.mercadolibre.com/users/{user_id}/items/search?search_type=scan&limit={limit}"
        else:
            url = f"https://api.mercadolibre.com/users/{user_id}/items/search?search_type=scan&scroll_id={scroll_id}"
            
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code != 200:
                print(f"❌ Error al listar items: {res.status_code} - {res.text}")
                break
            
            data = res.json()
            results = data.get('results', [])
            if not results:
                break
            
            item_ids.extend(results)
            print(f"⏳ Descargados {len(item_ids)} IDs de la API...")
            
            scroll_id = data.get('scroll_id')
            if not scroll_id:
                break
        except Exception as e:
            print(f"❌ Error de conexión al listar items: {str(e)}")
            break
            
    return list(set(item_ids))


def run_conciliation():
    # --- AUTENTICACIÓN GOOGLE ---
    print("🔍 [DEBUG] Iniciando conexión con Google Sheets...")
    sa_env = os.environ['GOOGLE_SERVICE_ACCOUNT'].strip().strip("'\"")
    sa_info = json.loads(sa_env)
    gc = gspread.service_account_from_dict(sa_info)
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # --- REFRESH TOKEN ML ---
    access_token = get_new_token(sh.worksheet(CONFIG_SHEET))
    if not access_token:
        print("❌ Error: No se pudo refrescar el token de ML. Deteniendo conciliación.")
        return

    # --- OBTENER USER ID ---
    user_id = obtener_user_id(access_token)
    if not user_id:
        print("❌ Error: No se pudo obtener el User ID del vendedor. Deteniendo conciliación.")
        return

    # --- OBTENER TODOS LOS ITEMS DESDE MERCADO LIBRE ---
    api_items = obtener_todos_items_api(user_id, access_token)
    print(f"✅ [DEBUG] Total de Item IDs activos/pausados en Mercado Libre: {len(api_items)}")

    # --- OBTENER REGISTROS DE LA HOJA ---
    worksheet = sh.worksheet(SHEET_NAME)
    df = pd.DataFrame(worksheet.get_all_records()).fillna('')
    original_columns = df.columns.tolist()
    
    # Filtrar solo IDs válidos que comiencen con MLM u otros prefijos de Mercado Libre
    df_items = df[df['Item ID'].astype(str).str.strip().str.match(r'^[A-Z]{3}\d+$', na=False)]
    sheet_items = df_items['Item ID'].unique().tolist()
    print(f"✅ [DEBUG] Total de Item IDs en Google Sheets: {len(sheet_items)}")

    # --- DETECTAR ELIMINADOS (Están en Sheets pero no en la API) ---
    api_items_set = set(api_items)
    items_a_eliminar = [x for x in sheet_items if x not in api_items_set]
    
    # --- DETECTAR NUEVOS (Están en la API pero no en Sheets) ---
    sheet_items_set = set(sheet_items)
    items_a_agregar = [x for x in api_items if x not in sheet_items_set]

    print(f"⚠️ [CONCILIACIÓN] Publicaciones en hoja no encontradas en API (por eliminar): {len(items_a_eliminar)}")
    print(f"✨ [CONCILIACIÓN] Publicaciones en API no encontradas en hoja (por agregar): {len(items_a_agregar)}")

    hubo_cambios = False

    # 1. ELIMINAR REGISTROS INEXISTENTES
    if items_a_eliminar:
        print(f"🔍 [DEBUG] Eliminando {len(items_a_eliminar)} publicaciones del DataFrame...")
        # Conservar solo filas cuyo Item ID no esté en la lista por eliminar
        df = df[~df['Item ID'].isin(items_a_eliminar)]
        hubo_cambios = True

    # 2. DESCARGAR DETALLES Y AGREGAR NUEVAS PUBLICACIONES
    if items_a_agregar:
        print(f"🔍 [DEBUG] Descargando detalles concurrentemente para {len(items_a_agregar)} nuevos items...")
        nuevos_detalles = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            f_to_id = {executor.submit(get_data, i_id, access_token): i_id for i_id in items_a_agregar}
            completed = 0
            for f in concurrent.futures.as_completed(f_to_id):
                res = f.result()
                if res:
                    nuevos_detalles[f_to_id[f]] = res
                completed += 1
                if completed % 100 == 0 or completed == len(items_a_agregar):
                    print(f"⏳ Descargando nuevos detalles: {completed}/{len(items_a_agregar)}...")

        # Generar nuevas filas
        nuevas_filas = []
        for it_id in items_a_agregar:
            if it_id in nuevos_detalles:
                data = nuevos_detalles[it_id]
                item = data['body']
                p_promo = data['promo_price']
                comision = data['comision']
                shipping_cost = data['shipping_cost']
                
                logistica_real = item.get('shipping', {}).get('logistic_type', 'not_specified')
                es_full = logistica_real == 'fulfillment'
                
                # Mapear variaciones si tiene
                variaciones = item.get('variations', [])
                
                if variaciones:
                    for v in variaciones:
                        v_id = str(v.get('id'))
                        stock_real = v.get('available_quantity', 0)
                        
                        # Estatus basado en lógica
                        api_status = item.get('status')
                        if not isinstance(api_status, str):
                            api_status = str(api_status) if api_status is not None else ""
                        sub_status_list = item.get('sub_status', [])
                        es_revision = "under_review" in sub_status_list or "waiting_for_patch" in sub_status_list or api_status == 'under_review'
                        
                        if es_revision:
                            nuevo_estatus = "Bajo revision"
                        elif api_status == 'active':
                            nuevo_estatus = "Activa" if stock_real > 0 else "Pausada"
                        elif api_status == 'paused':
                            nuevo_estatus = "Pausada"
                        elif api_status == 'closed':
                            nuevo_estatus = "Cerrada"
                        else:
                            nuevo_estatus = api_status.capitalize() if api_status else "Pausada"

                        sku = obtener_sku(v)
                        if not sku:
                            sku = obtener_sku(item)
                            
                        # Construir fila
                        fila = {col: "" for col in original_columns}
                        fila['Item ID'] = it_id
                        fila['Variant ID'] = v_id
                        fila['ID_UNICO_VARIANTE'] = f"{it_id}_{v_id}"
                        fila['Titulo'] = item.get('title', '')
                        fila['Precio Base'] = float(item.get('original_price') or item.get('price') or 0.0)
                        fila['Precio Promo'] = float(p_promo)
                        fila['Comision'] = float(comision)
                        fila['Logistica'] = logistica_real
                        fila['CostoEnvio'] = shipping_cost if shipping_cost is not None else ""
                        fila['Stock (Solo Full)'] = int(stock_real) if es_full else 0
                        fila['Estatus'] = nuevo_estatus
                        fila['Att_SellerSKU'] = sku
                        fila['URL'] = item.get('permalink', '')
                        fila['SKU_1'] = sku
                        fila['Cant_1'] = 1 if sku else ""
                        
                        nuevas_filas.append(fila)
                else:
                    # Producto sin variaciones
                    stock_real = item.get('available_quantity', 0)
                    
                    api_status = item.get('status')
                    if not isinstance(api_status, str):
                        api_status = str(api_status) if api_status is not None else ""
                    sub_status_list = item.get('sub_status', [])
                    es_revision = "under_review" in sub_status_list or "waiting_for_patch" in sub_status_list or api_status == 'under_review'
                    
                    if es_revision:
                        nuevo_estatus = "Bajo revision"
                    elif api_status == 'active':
                        nuevo_estatus = "Activa" if stock_real > 0 else "Pausada"
                    elif api_status == 'paused':
                        nuevo_estatus = "Pausada"
                    elif api_status == 'closed':
                        nuevo_estatus = "Cerrada"
                    else:
                        nuevo_estatus = api_status.capitalize() if api_status else "Pausada"

                    sku = obtener_sku(item)
                    
                    fila = {col: "" for col in original_columns}
                    fila['Item ID'] = it_id
                    fila['Variant ID'] = ""
                    fila['ID_UNICO_VARIANTE'] = f"{it_id}_"
                    fila['Titulo'] = item.get('title', '')
                    fila['Precio Base'] = float(item.get('original_price') or item.get('price') or 0.0)
                    fila['Precio Promo'] = float(p_promo)
                    fila['Comision'] = float(comision)
                    fila['Logistica'] = logistica_real
                    fila['CostoEnvio'] = shipping_cost if shipping_cost is not None else ""
                    fila['Stock (Solo Full)'] = int(stock_real) if es_full else 0
                    fila['Estatus'] = nuevo_estatus
                    fila['Att_SellerSKU'] = sku
                    fila['URL'] = item.get('permalink', '')
                    fila['SKU_1'] = sku
                    fila['Cant_1'] = 1 if sku else ""
                    
                    nuevas_filas.append(fila)
        
        if nuevas_filas:
            print(f"🔍 [DEBUG] Agregando {len(nuevas_filas)} nuevas filas correspondientes a las variantes/productos detectados...")
            df_nuevos = pd.DataFrame(nuevas_filas)
            df = pd.concat([df, df_nuevos], ignore_index=True)
            hubo_cambios = True

    # 3. GUARDAR CAMBIOS DE VUELTA A GOOGLE SHEETS
    if hubo_cambios:
        print("🔍 [DEBUG] Guardando cambios de catálogo actualizados en Google Sheets...")
        df_cleaned = df.fillna("").astype(str).values.tolist()
        worksheet.update(values=[df.columns.values.tolist()] + df_cleaned, range_name='A1')
        print("✅ [DEBUG] Hoja 'PUBLICACIONES' conciliada y actualizada con éxito.")
    else:
        print("🔍 [DEBUG] No se requiere realizar cambios. El catálogo en Google Sheets está perfectamente sincronizado con Mercado Libre.")

if __name__ == "__main__":
    run_conciliation()
