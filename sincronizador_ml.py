import pandas as pd
import requests
import concurrent.futures
import gspread
import os
import json
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import sys
import time
from gspread.exceptions import APIError

# Configurar la consola para soportar caracteres UTF-8 (emojis) en Windows
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass
if hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8')
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

    if not os.path.exists(env_file):
        print(f"❌ Error: El archivo de configuración '{env_file}' no existe en esta computadora.")
        print("Para poder ejecutar el script para la Tienda 2 de forma local, debes crear ese archivo con las credenciales de la Tienda 2.")
        sys.exit(1)

    print(f"ℹ️ [DEBUG] Cargando variables desde: {env_file}")
    load_dotenv(env_file)

# --- CONFIGURACIÓN PRINCIPAL ---
SPREADSHEET_ID = os.environ['SPREADSHEET_ID'] 
SHEET_NAME = 'PUBLICACIONES'
CONFIG_SHEET = 'Config_ML'
HISTORY_SHEET = 'Historial'

# Mantenemos las 48 horas para detectar cambios
HORAS_ATRAS = 48 

# --- CONFIGURACIÓN DISCORD ---
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

def call_with_retry(func, *args, retries=3, delay=2, backoff=2, **kwargs):
    """Ejecuta una función que interactúa con Google Sheets con intentos de reintento en caso de error transitorio o de red."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            status_code = getattr(e, 'code', None)
            try:
                if not status_code and hasattr(e, 'response') and e.response is not None:
                    status_code = e.response.status_code
            except:
                pass
            
            is_transient = True
            if status_code:
                try:
                    status_code = int(status_code)
                    if status_code not in [429, 500, 502, 503, 504]:
                        is_transient = False
                except:
                    pass
            
            if not is_transient or attempt == retries - 1:
                print(f"❌ Error definitivo de Google Sheets API tras intentos: {e}")
                raise e
            
            print(f"⚠️ [Google Sheets API] Error detectado ({e}). Reintentando en {delay} segundos (intento {attempt + 1}/{retries})...")
            time.sleep(delay)
            delay *= backoff
        except Exception as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise e
            if attempt == retries - 1:
                print(f"❌ Error definitivo de red/conexión al Sheets API tras intentos: {e}")
                raise e
            print(f"⚠️ [Google Sheets API] Error de conexión ({e}). Reintentando en {delay} segundos (intento {attempt + 1}/{retries})...")
            time.sleep(delay)
            delay *= backoff

def read_config_row(config_ws):
    """Lee las credenciales en lote para reducir llamadas de API."""
    res = config_ws.get('A2:C2')
    if res and len(res) > 0:
        row = res[0]
        while len(row) < 3:
            row.append('')
        return row
    return [config_ws.acell('A2').value, config_ws.acell('B2').value, config_ws.acell('C2').value]

def get_new_token(config_ws):
    """Refresca el token de acceso de Mercado Libre con depuración activa."""
    try:
        # Extraer credenciales limpiando espacios y formatos extraños
        row_vals = call_with_retry(read_config_row, config_ws)
        c_id = str(row_vals[0] or '').replace(',', '').replace(' ', '').strip()
        c_secret = str(row_vals[1] or '').strip()
        r_token = str(row_vals[2] or '').strip()
        
        print(f"🔍 [DEBUG] Intentando refrescar token... Client ID detectado: {c_id}")
        
        url = "https://api.mercadolibre.com/oauth/token"
        res = requests.post(url, data={
            'grant_type': 'refresh_token', 
            'client_id': c_id, 
            'client_secret': c_secret, 
            'refresh_token': r_token
        })
        
        print(f"🔍 [DEBUG] Código de estado de Mercado Libre: {res.status_code}")
        
        if res.status_code == 200:
            token_data = res.json()
            # Guardamos el nuevo refresh token devuelto por la API
            call_with_retry(config_ws.update_acell, 'C2', token_data['refresh_token'])
            print("✅ Token refrescado y guardado con éxito en la celda C2.")
            return token_data['access_token']
        else:
            print(f"❌ Error de la API de Mercado Libre: {res.text}")
            return None
    except Exception as e: 
        print(f"❌ Error interno al intentar leer la hoja o conectar con la API: {str(e)}")
        return None

def obtener_sku(item_or_variation):
    """Busca el SKU (SELLER_SKU) dentro de los atributos o en seller_custom_field."""
    if not item_or_variation:
        return ''
    # 1. Intentar buscar en attributes con id 'SELLER_SKU'
    attrs = item_or_variation.get('attributes', [])
    for attr in attrs:
        if attr.get('id') == 'SELLER_SKU':
            val = str(attr.get('value_name') or attr.get('value_struct', '') or '').strip()
            if val:
                return val
                
    # 2. Si no se encontró o está vacío, intentar seller_custom_field
    custom_field = item_or_variation.get('seller_custom_field')
    if custom_field:
        return str(custom_field).strip()
        
    return ''

def get_data(i_id, token, user_id=None):
    """Obtiene detalles del item, precio promocional, comisión y costo de envío."""
    headers = {'Authorization': f'Bearer {token}'}
    try:
        # Detalle del ítem
        it_res = requests.get(f"https://api.mercadolibre.com/items/{i_id}", headers=headers, timeout=15)
        if it_res.status_code != 200:
            return None
        it = it_res.json()
        
        # Precio promocional (el que ve el cliente)
        sp = requests.get(f"https://api.mercadolibre.com/items/{i_id}/sale_price", headers=headers, timeout=15).json()
        p_promo = sp.get('amount') or it.get('price') or 0
        
        # --- CONSULTA DE COMISIÓN BASADA EN PRECIO DE PROMO ---
        cat_id = it.get('category_id')
        l_type = it.get('listing_type_id')
        site_id = it.get('site_id', 'MLM')
        comision = 0
        
        # Usamos p_promo en lugar de it.get('price') para que el cálculo sea real
        if p_promo and cat_id and l_type:
            comm_url = f"https://api.mercadolibre.com/sites/{site_id}/listing_prices?price={p_promo}&category_id={cat_id}&listing_type_id={l_type}"
            comm_res = requests.get(comm_url, headers=headers, timeout=10).json()
            
            if isinstance(comm_res, list) and len(comm_res) > 0:
                comision = comm_res[0].get('sale_fee_amount', 0)
            elif isinstance(comm_res, dict):
                comision = comm_res.get('sale_fee_amount', 0)

        # --- CONSULTA DE COSTO DE ENVÍO ---
        shipping_cost = None
        shipping_info = it.get('shipping', {})
        shipping_mode = shipping_info.get('mode')
        is_free_shipping = shipping_info.get('free_shipping', False)

        if shipping_mode == 'me2' and (is_free_shipping or (p_promo and p_promo < 299)):
            if user_id:
                try:
                    # Usar el endpoint de simulación de costo de envío gratis del vendedor
                    ship_url = f"https://api.mercadolibre.com/users/{user_id}/shipping_options/free?item_id={i_id}"
                    ship_res = requests.get(ship_url, headers=headers, timeout=10)
                    if ship_res.status_code == 200:
                        ship_data = ship_res.json()
                        coverage = ship_data.get('coverage', {})
                        all_country = coverage.get('all_country', {})
                        shipping_cost = all_country.get('list_cost')
                        if shipping_cost is not None:
                            shipping_cost = float(shipping_cost)
                except Exception as e:
                    print(f"⚠️ [DEBUG] No se pudo obtener el costo de envío con el nuevo endpoint para {i_id}: {str(e)}")

            # Fallback al método anterior si no se obtuvo con el nuevo endpoint o si no hay user_id
            if shipping_cost is None:
                try:
                    ship_url = f"https://api.mercadolibre.com/items/{i_id}/shipping_options"
                    ship_res = requests.get(ship_url, headers=headers, timeout=10)
                    if ship_res.status_code == 200:
                        ship_data = ship_res.json()
                        options = ship_data.get('options', [])
                        # Intentar buscar la opción con free_shipping = True
                        for opt in options:
                            if opt.get('free_shipping') is True:
                                shipping_cost = float(opt.get('list_cost', 0.0))
                                break
                        # Si no hay envío gratis, tomar el list_cost de la primera opción
                        if shipping_cost is None and options:
                            shipping_cost = float(options[0].get('list_cost', 0.0))
                except Exception as e:
                    print(f"⚠️ [DEBUG] Fallback: No se pudo obtener el costo de envío para {i_id}: {str(e)}")
        else:
            shipping_cost = ""

        return {'body': it, 'promo_price': p_promo, 'comision': comision, 'shipping_cost': shipping_cost}
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
    datos_actuales = call_with_retry(h_ws.get_all_values)
    encabezado = ["ID_UNICO_VARIANTE", "Cual fue el cambio", "Ultima Modificacion ML"]
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
    call_with_retry(h_ws.clear)
    call_with_retry(h_ws.update, values=[encabezado] + todos_limitados, range_name='A1')

def run_update():
    # --- AUTENTICACIÓN GOOGLE ---
    sa_env = os.environ['GOOGLE_SERVICE_ACCOUNT'].strip().strip("'\"")
    sa_info = json.loads(sa_env)
    gc = gspread.service_account_from_dict(sa_info)
    sh = call_with_retry(gc.open_by_key, SPREADSHEET_ID)
    
    # --- REFRESH TOKEN ML ---
    config_ws = call_with_retry(sh.worksheet, CONFIG_SHEET)
    access_token = get_new_token(config_ws)
    if not access_token: 
        print("Error: No se pudo refrescar el token de ML. Deteniendo la ejecución.")
        return

    # --- OBTENER USER ID ---
    user_id = None
    try:
        me_res = requests.get("https://api.mercadolibre.com/users/me", headers={'Authorization': f'Bearer {access_token}'}, timeout=15)
        if me_res.status_code == 200:
            user_id = me_res.json().get('id')
            print(f"✅ [DEBUG] User ID del vendedor obtenido: {user_id}")
    except Exception as e:
        print(f"⚠️ [DEBUG] No se pudo obtener el User ID del vendedor: {str(e)}")

    # --- OBTENER DATOS DE LA HOJA ---
    print("🔍 [DEBUG] Obteniendo registros de la pestaña 'PUBLICACIONES'...")
    worksheet = call_with_retry(sh.worksheet, SHEET_NAME)
    records = call_with_retry(worksheet.get_all_records)
    df = pd.DataFrame(records).fillna('')
    
    # Omitir de la consulta los registros cuyo Estatus en la hoja sea 'Cerrada'
    df_a_consultar = df[df['Estatus'].astype(str).str.strip().str.capitalize() != 'Cerrada']
    unique_ids = df_a_consultar['Item ID'].unique().tolist()
    # Limpiar IDs vacíos si existen
    unique_ids = [str(x).strip() for x in unique_ids if x]
    print(f"✅ [DEBUG] Datos de la hoja cargados. Filas totales: {len(df)}. IDs únicos a consultar (excluyendo 'Cerrada'): {len(unique_ids)}")

    # --- DESCARGA CONCURRENTE ---
    item_details = {}
    total_ids = len(unique_ids)
    print(f"🔍 [DEBUG] Descargando información de {total_ids} items desde la API de Mercado Libre...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        f_to_id = {executor.submit(get_data, i_id, access_token, user_id): i_id for i_id in unique_ids}
        completed_count = 0
        for f in concurrent.futures.as_completed(f_to_id):
            res = f.result()
            if res: 
                item_details[f_to_id[f]] = res
            completed_count += 1
            if completed_count % 100 == 0 or completed_count == total_ids:
                print(f"⏳ [DEBUG] Progreso: {completed_count}/{total_ids} items descargados...")


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

            logistica_real = item.get('shipping', {}).get('logistic_type', 'not_specified')
            sheet_logistica = str(row.get('Logistica', '')).strip()
            nueva_comision = float(data.get('comision', 0.0))

            # Stock real
            if v_id and v_id not in ['0', '', 'None']:
                v_data = next((v for v in item.get('variations', []) if str(v.get('id')) == v_id), None)
                stock_real = v_data.get('available_quantity', 0) if v_data else 0
            else:
                stock_real = item.get('available_quantity', 0)

            es_full = logistica_real == 'fulfillment'
            # Nuevo Estatus (Nuevo mapeo con relación y condición de stock activa)
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

            sheet_estatus = str(row['Estatus']).strip().capitalize() 

            # Sub-status para Discord
            sub_status_str = ", ".join(sub_status_list) if sub_status_list else "N/A"
            if sheet_estatus == "Activa":
                if nuevo_estatus == "Pausada" or es_revision:
                    razon = f"⚠️ REVISIÓN ({sub_status_str})" if es_revision else f"Pausada ({sub_status_str})"
                    titulo_caja = item.get('title', 'Producto')[:30]
                    alertas_para_discord.append(f"• {it_id} | {titulo_caja:<30} | {razon}")
                    
            # Costo de Envío (actualización y preservación)
            nuevo_p_envio = data.get('shipping_cost')
            
            # Si la API no devolvió costo (por estar pausada/cerrada/error), mantenemos el costo actual en la hoja
            if nuevo_p_envio is None:
                nuevo_p_envio = row.get('CostoEnvio', '')

            # Comparación segura de flotantes
            try:
                sheet_envio_val = float(row.get('CostoEnvio', 0.0)) if row.get('CostoEnvio', '') != '' else 0.0
            except:
                sheet_envio_val = 0.0

            try:
                nuevo_envio_val = float(nuevo_p_envio) if nuevo_p_envio not in ['', None] else 0.0
            except:
                nuevo_envio_val = 0.0

            envio_cambio = (abs(sheet_envio_val - nuevo_envio_val) > 0.01)

            # Título y SKU reales de la API
            api_titulo = item.get('title', '').strip()
            
            # Obtener el SKU de la variación o del item principal
            if v_id and v_id not in ['0', '', 'None']:
                v_data = next((v for v in item.get('variations', []) if str(v.get('id')) == v_id), None)
                api_sku = obtener_sku(v_data) if v_data else obtener_sku(item)
            else:
                api_sku = obtener_sku(item)
                
            sheet_titulo = str(row.get('Titulo', '')).strip()
            sheet_sku = str(row.get('Att_SellerSKU', '')).strip()

            # Solo detectar cambios si la API devuelve un valor no vacío (para evitar borrar datos locales válidos)
            titulo_cambio = bool(api_titulo and sheet_titulo != api_titulo)
            sku_cambio = bool(api_sku and sheet_sku != api_sku)

            # Detección de cambios
            nuevo_p_promo = float(data.get('promo_price') or 0.0)
            nuevo_stock = int(stock_real) if es_full else 0
            nuevo_p_base = float(item.get('original_price') or item.get('price') or 0.0)
            
            sheet_promo = float(row['Precio Promo']) if row['Precio Promo'] != '' else 0.0
            sheet_stock = int(row['Stock (Solo Full)']) if row['Stock (Solo Full)'] != '' else 0
            sheet_comision = float(row.get('Comision', 0.0)) if row.get('Comision', '') != '' else 0.0

            cambio_en_fila = (
                (sheet_estatus != nuevo_estatus) or 
                (abs(sheet_promo - nuevo_p_promo) > 0.01) or 
                (sheet_stock != nuevo_stock) or
                (sheet_logistica != logistica_real) or
                (abs(sheet_comision - nueva_comision) > 0.01) or
                envio_cambio or
                titulo_cambio or
                sku_cambio
            )

            if cambio_en_fila:
                df.at[i, 'Precio Base'] = nuevo_p_base
                df.at[i, 'Precio Promo'] = nuevo_p_promo
                df.at[i, 'Stock (Solo Full)'] = nuevo_stock
                df.at[i, 'Estatus'] = nuevo_estatus
                df.at[i, 'Logistica'] = logistica_real
                df.at[i, 'Comision'] = nueva_comision
                df.at[i, 'CostoEnvio'] = nuevo_p_envio
                if titulo_cambio:
                    df.at[i, 'Titulo'] = api_titulo
                if sku_cambio:
                    df.at[i, 'Att_SellerSKU'] = api_sku
                hubo_cambios = True

                last_up_str = item.get('last_updated', '').replace('Z', '+00:00')
                try:
                    fecha_mod_ml = datetime.fromisoformat(last_up_str)
                    if fecha_mod_ml > limite_historial:
                        cambios = []
                        if sheet_estatus != nuevo_estatus: cambios.append(f"Stat: {sheet_estatus}->{nuevo_estatus}")
                        if sheet_stock != nuevo_stock: cambios.append(f"Stock: {sheet_stock}->{nuevo_stock}")
                        if abs(sheet_promo - nuevo_p_promo) > 0.01: cambios.append(f"P: {sheet_promo}->{nuevo_p_promo}")
                        if abs(sheet_comision - nueva_comision) > 0.01: cambios.append(f"Com: {sheet_comision}->{nueva_comision}")
                        if envio_cambio: cambios.append(f"Envio: {sheet_envio_val}->{nuevo_envio_val}")
                        if titulo_cambio: cambios.append(f"Titulo: {sheet_titulo}->{api_titulo}")
                        if sku_cambio: cambios.append(f"SKU: {sheet_sku}->{api_sku}")
                        
                        v_clean = v_id if v_id and v_id not in ['0', '', 'None'] else ""
                        id_unico = f"{it_id}_{v_clean}"
                        log_reporte.append([id_unico, " | ".join(cambios), fecha_mod_ml.strftime("%d/%m/%Y %H:%M")])
                except: continue

    # --- GUARDAR EN GOOGLE SHEETS ---
    if hubo_cambios:
        print("🔍 [DEBUG] Detectados cambios en los productos. Guardando actualización en Google Sheets...")
        df_cleaned = df.fillna("").astype(str).values.tolist()
        call_with_retry(worksheet.clear)
        call_with_retry(worksheet.update, values=[df.columns.values.tolist()] + df_cleaned, range_name='A1')
        print("✅ [DEBUG] Hoja 'PUBLICACIONES' actualizada con éxito.")
        if log_reporte:
            print(f"🔍 [DEBUG] Escribiendo {len(log_reporte)} registros en el Historial...")
            worksheets = call_with_retry(sh.worksheets)
            h_ws = next((w for w in worksheets if w.title.strip() == HISTORY_SHEET), None)
            if h_ws: 
                actualizar_historial_limpio(h_ws, log_reporte)
                print("✅ [DEBUG] Historial actualizado.")
    else:
        print("🔍 [DEBUG] No se detectó ningún cambio. La hoja de cálculo está al día.")

    # --- ENVÍO A DISCORD ---
    if alertas_para_discord:
        print(f"🔍 [DEBUG] Enviando {len(alertas_para_discord)} alertas a Discord...")
        for grupo in [alertas_para_discord[x:x+15] for x in range(0, len(alertas_para_discord), 15)]:
            enviar_alerta_discord(f"⚠️ **RESUMEN DE CAMBIOS ({datetime.now().strftime('%H:%M')})** ⚠️\n```yaml\n" + "\n".join(grupo) + "\n```")
        print("✅ [DEBUG] Alertas enviadas a Discord.")

    print("🎉 [DEBUG] Proceso de sincronización finalizado exitosamente.")

if __name__ == "__main__":
    run_update()