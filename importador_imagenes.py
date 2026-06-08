import os
import sys
import json
import re
import requests
from dotenv import load_dotenv
import gspread
from gspread.cell import Cell

# Configurar la consola para soportar caracteres UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# Cargar variables de entorno desde el archivo .env
load_dotenv(".env")

# Importar configuraciones y helpers desde sincronizador_ml
sys.path.append(".")
from sincronizador_ml import get_new_token, SPREADSHEET_ID

def run_image_import():
    # --- AUTENTICACIÓN GOOGLE ---
    print("🔍 Conectando con Google Sheets...")
    sa_env = os.environ['GOOGLE_SERVICE_ACCOUNT'].strip().strip("'\"")
    sa_info = json.loads(sa_env)
    gc = gspread.service_account_from_dict(sa_info)
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # --- REFRESH TOKEN ML ---
    access_token = get_new_token(sh.worksheet('Config_ML'))
    if not access_token:
        print("❌ Error: No se pudo refrescar el token de ML. Deteniendo la ejecución.")
        return

    # --- LEER HOJA CATALOGO ---
    print("🔍 Obteniendo datos de la pestaña 'CATALOGO'...")
    ws = sh.worksheet('CATALOGO')
    rows = ws.get_all_values()
    if len(rows) <= 1:
        print("ℹ️ Hoja vacía o solo contiene cabecera.")
        return
        
    headers = rows[0]
    
    # Obtener índices de columnas (1-indexed)
    try:
        sku_idx = headers.index('Sku') + 1
        imagen_idx = headers.index('Imagen') + 1
        link_idx = headers.index('LINK ML') + 1
    except ValueError as e:
        print(f"❌ Error: No se encontraron las columnas necesarias en la cabecera: {str(e)}")
        return

    print(f"✅ Columnas encontradas: Sku (col {sku_idx}), Imagen (col {imagen_idx}), LINK ML (col {link_idx})")
    
    headers_len = len(headers)
    cells_to_update = []
    total_processed = 0
    total_updated = 0
    
    print("🔍 Buscando filas que no tengan imagen pero sí LINK ML...")
    
    for r_idx in range(1, len(rows)):
        row_num = r_idx + 1  # 1-based index in Google Sheets
        row_data = rows[r_idx]
        
        # Rellenar con vacíos si la fila tiene menos columnas que la cabecera
        while len(row_data) < headers_len:
            row_data.append('')
            
        sku = str(row_data[sku_idx - 1]).strip()
        imagen = str(row_data[imagen_idx - 1]).strip()
        link_ml = str(row_data[link_idx - 1]).strip()
        
        # Ignorar si ya tiene imagen
        if imagen:
            continue
            
        # Ignorar si no tiene link de Mercado Libre
        if not link_ml:
            continue
            
        # Extraer Item ID del link
        match = re.search(r'(ML[A-Z])[-_]?(\d+)', link_ml)
        if not match:
            print(f"⚠️ [Fila {row_num}] Link no válido o no se pudo extraer el ID de ML: '{link_ml}'")
            continue
            
        item_id = match.group(1) + match.group(2)
        total_processed += 1
        
        print(f"⏳ [Fila {row_num}] Consultando {item_id} para SKU '{sku}'...")
        
        # Consultar API de Mercado Libre
        headers_api = {'Authorization': f'Bearer {access_token}'}
        try:
            res = requests.get(f"https://api.mercadolibre.com/items/{item_id}", headers=headers_api, timeout=15)
            if res.status_code == 200:
                item_data = res.json()
                pictures = item_data.get('pictures', [])
                if pictures:
                    # Preferir secure_url, si no url
                    pic_url = pictures[0].get('secure_url') or pictures[0].get('url')
                    if pic_url:
                        cells_to_update.append(Cell(row=row_num, col=imagen_idx, value=pic_url))
                        total_updated += 1
                        print(f"   ✅ Imagen encontrada: {pic_url}")
                    else:
                        print(f"   ⚠️ No se encontró URL en las fotos de {item_id}")
                else:
                    print(f"   ⚠️ La publicación {item_id} no tiene imágenes en la API")
            else:
                print(f"   ❌ Error API ML {res.status_code}: {res.text[:100]}")
        except Exception as e:
            print(f"   ❌ Error al conectar con API ML para {item_id}: {str(e)}")
            
        # Pequeña pausa para no saturar la API
        if total_processed % 30 == 0:
            import time
            time.sleep(1)

    # --- BATCH UPDATE EN GOOGLE SHEETS ---
    if cells_to_update:
        print(f"\n💾 Guardando {len(cells_to_update)} links de imágenes en la pestaña 'CATALOGO'...")
        # Dividir actualizaciones en lotes de 100 para evitar límites de tasa del API de Google Sheets
        batch_size = 100
        for i in range(0, len(cells_to_update), batch_size):
            batch = cells_to_update[i:i + batch_size]
            ws.update_cells(batch, value_input_option='USER_ENTERED')
            print(f"   ✅ Lote {i // batch_size + 1} de {((len(cells_to_update) - 1) // batch_size) + 1} guardado.")
        print("🎉 Proceso finalizado exitosamente.")
    else:
        print("\nℹ️ No se encontraron nuevos links de imágenes para importar.")

if __name__ == "__main__":
    run_image_import()
