import requests
import pandas as pd
import sqlite3
import concurrent.futures

from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import *
# Pandas ogohlantirishini o'chirish
pd.set_option('future.no_silent_downcasting', True)

# --- GLOBAL O'ZGARUVCHILAR ---
MY_SHOPS = ['ANDALUS', 'BERUNIY MEN', 'Dressco Integro', 'MAGNIT MEN', 'SHAXRISTON']

SHOP_MAP = {
    "31f89356-817d-4a07-abff-6edb45002801": "Dressco Integro",
    "b7889973-6162-4358-a083-04c685404070": "ANDALUS",
    "2fb7c502-4694-4f38-ab3c-76ef6a3bc73b": "BERUNIY MEN",
    "ea77b256-1e3d-4e40-9cb9-fd6048669c99": "MAGNIT MEN",
    "6dd93ef3-e555-4c93-b119-b34b98d68d07": "SHAXRISTON",
    "62d5698c-6cde-4989-9040-07b8729a9c09": "SKLAD_PRIHODA",
    "SKLAD_PRIHODA": "SKLAD_PRIHODA",
    "СКЛАД ПРИХОДА": "SKLAD_PRIHODA",
    "29b247c7-e7a6-4e79-95c2-ce97a6e8b757": "BUTTON SKLAD MEN",
    "c91a913b-c295-4775-a7a8-4a0ce2578fa0": "СКЛАД БРАКА"
}
ID_TO_NAME = {k: v for k, v in SHOP_MAP.items()}
# Pandas ogohlantirishini o'chirish
pd.set_option('future.no_silent_downcasting', True)

# --- SESSION ---
def get_session():
    s = requests.Session()
    # total=5 ga oshiring (oldin 3 edi)
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

session = get_session()

def login_admin():
    try:
        r = session.post(
            f"{ADMIN_BASE}/v1/auth/login",
            json={"secret_token": SECRET_KEY},
            headers={"accept": "application/json", "content-type": "application/json"},
            timeout=15
        )
        if r.status_code == 200:
            return r.json().get("data", {}).get("access_token")
    except Exception as e:
        print(f"Login Error: {e}")
    return None

# --- 1. KATALOGNI TO'LIQ YANGILASH (Narx va Materiallar bilan) ---
def update_catalog_only():
    print("🚀 Katalog yangilanishi boshlandi (Full Mode)...")
    token = login_admin()
    if not token: return False, "❌ Admin token xatosi."

    all_items = []
    page = 1
    per_page = 1000

    while True:
        try:
            r = session.get(
                f"{ADMIN_BASE}/v2/products",
                params={"limit": per_page, "page": page},
                headers={"authorization": f"Bearer {token}"},
                timeout=45
            )
            data = r.json().get("products", [])
            if not data: break
            all_items.extend(data)
            print(f"   📄 Sahifa {page}: {len(data)} ta mahsulot...")
            if len(data) < per_page: break
            page += 1
        except Exception as e:
            print(f"❌ Xatolik: {e}")
            break

    if not all_items:
        return False, "❌ Mahsulotlar topilmadi."

    processed_data = []
    for p in all_items:
        try:
            pid = p.get('id', '')
            if not pid: continue

            # --- CUSTOM FIELDS ---
            custom_fields = p.get('custom_fields') or []

            def get_cf(name):
                for f in custom_fields:
                    if f.get('custom_field_name') == name:
                        val = f.get('custom_field_value', '')
                        if name in ['import_date', 'Дата'] and '-' in str(val):
                            return str(val).split('-')[-1].strip()
                        return val
                return ''

            sub_cat = get_cf('Подкатегория')
            if sub_cat == '111':
                sub_cat = 'Кроссовки-Casual'
            elif sub_cat == 'Рубашка классическая':
                sub_cat = 'Рубашка класс.дл/р'

            
            if not sub_cat:
                sub_cat = 'Boshqa'

            # --- PRICES ---
            shop_prices = p.get("shop_prices") or []
            retail = 0; supply = 0; promo = 0
            if shop_prices:
                # Birinchi narxni olamiz
                retail = shop_prices[0].get('retail_price', 0)
                supply = shop_prices[0].get('supply_price', 0)
                promo = shop_prices[0].get('promo_price', 0)

            # --- STOCK ---
            current_stock = sum([(s.get("active_measurement_value") or 0) for s in p.get("shop_measurement_values") or []])

            # --- SUPPLIER ---
            sup = p.get("suppliers") or []
            sup_name = sup[0].get('name', '') if sup else ''

            processed_data.append({
                'product_id': pid,
                'Kategoriya': p.get('categories', [{}])[0].get('name', 'Boshqa'),
                'Подкатегория': sub_cat,
                'Баркод': p.get('barcode', ''),
                'Цвет': get_cf('Цвет') or '-',
                'Материал': get_cf('Материал') or '-',  # <--- YANGI
                'Вид': get_cf('Вид') or '-',            # <--- YANGI
                'Sotuv_Narxi': retail,                  # <--- YANGI
                'Tannarx': supply,                      # <--- YANGI
                'Aksiya_Narxi': promo,                  # <--- YANGI
                'Артикул': p.get('sku', ''),
                'Наименование': p.get('name', ''),
                'import_date': get_cf('import_date') or get_cf('Дата'),
                'Поставщик': sup_name,
                'Qoldiq': current_stock
            })
        except: continue

    conn = sqlite3.connect(DB_FILE)
    df = pd.DataFrame(processed_data)

    # Eskisini to'liq o'chirib, yangi struktura bilan yozamiz
    df.to_sql("d_Mahsulotlar", conn, if_exists="replace", index=False)
    conn.close()

    return True, f"✅ Katalog to'liq yangilandi! Jami: {len(df)} ta mahsulot. (Narxlar qo'shildi)"

def get_main_categories():
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql("SELECT DISTINCT Kategoriya FROM d_Mahsulotlar ORDER BY Kategoriya", conn)
        conn.close()
        return df['Kategoriya'].dropna().tolist()
    except: return []

def get_subcategories_by_cat(main_cat):
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql("SELECT DISTINCT Подкатегория FROM d_Mahsulotlar WHERE Kategoriya = ? ORDER BY Подкатегория", conn, params=(main_cat,))
        conn.close()
        return df['Подкатегория'].dropna().tolist()
    except: return []

# --- 2. TARIX VA ANALIZ ---
def fetch_history_single(product_id):
    # E'tibor bering: url da endi .io ishlatiladi (tepadagi o'zgaruvchidan olinadi)
    url = f"{HISTORY_BASE_URL}/api/v2/product-movement/{product_id}"

    headers = {
        "authorization": BROWSER_TOKEN,
        "platform-id": PLATFORM_ID,
        "cookie": COOKIE_VALUE,
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru",
        "referer": "https://buttonshop.billz.io/products/catalog",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    }
    clean_rows = []
    page = 1
    while True:
        try:
            r = session.get(url, params={"limit": 100, "page": page}, headers=headers, timeout=20)
            if r.status_code == 200:
                data = r.json()
                movements = data.get('movements') or []
                if not movements: break
                for move in movements:
                    clean_rows.append({
                        'product_id': product_id,
                        'sana': move.get('created_at', ''),
                        'turi': move.get('type', ''),
                        'miqdor': move.get('measurement_value', 0),
                        'from_shop_id': move.get('from_shop', ''),
                        'to_shop_id': move.get('to_shop', '')
                    })
                page += 1
            else: break
        except: break
    return clean_rows


def run_markdown_analysis(target_subcat, progress_callback=None):
    print(f"📉 SKIDKA ANALIZ (STR va Sotuv bilan): {target_subcat}")
    TODAY = datetime.now()

    conn = sqlite3.connect(DB_FILE)

    # 1. MAHSULOTLARNI OLISH (API dan kelgan tayyor Qoldiq bilan)
    try:
        query_prod = """
            SELECT 
                product_id, Подкатегория, Артикул, Баркод, Цвет, 
                Sotuv_Narxi, Aksiya_Narxi, Tannarx, 
                Поставщик, Вид, Материал, Наименование, Qoldiq
            FROM d_Mahsulotlar 
            WHERE Подкатегория = ?
        """
        df_prod = pd.read_sql(query_prod, conn, params=(target_subcat,))
    except Exception as e:
        conn.close()
        return None, f"❌ Bazada xatolik: {e}"

    if df_prod.empty:
        conn.close()
        return None, "❌ Bu podkategoriyada tovar yo'q."

    product_ids = df_prod['product_id'].tolist()

    # 2. TARIXNI OLISH
    if product_ids:
        placeholders = ','.join('?' * len(product_ids))
        # miqdor ustuni ham kerak (sotuv sonini hisoblash uchun)
        query_hist = f"SELECT product_id, sana, turi, miqdor, from_shop_id FROM d_History WHERE product_id IN ({placeholders})"
        df_history = pd.read_sql(query_hist, conn, params=product_ids)
    else:
        df_history = pd.DataFrame(columns=['product_id', 'sana', 'turi', 'miqdor', 'from_shop_id'])
    
    conn.close()

    # 3. DATA TAYYORLASH
    df_history['sana'] = pd.to_datetime(df_history['sana'])
    df_history['miqdor'] = pd.to_numeric(df_history['miqdor'], errors='coerce').fillna(0).abs()
    
    # Bizning do'konlar IDsi
    my_shop_ids = [k for k, v in SHOP_MAP.items() if v in MY_SHOPS]
    
    # A) SOTUVLARNI FILTRLASH
    # Shart: Turi = Sale/Order va Qayerdan = Bizning do'kon
    df_history['is_sale'] = (
        df_history['turi'].astype(str).str.contains("sale|order|sotuv|продажа", case=False, na=False) &
        df_history['from_shop_id'].isin(my_shop_ids)
    )

    # B) KIRIM (Yoshi uchun)
    df_history['is_arrival'] = (
        df_history['turi'].astype(str).str.contains("import|kirim", case=False, na=False) |
        (df_history['from_shop_id'].str.contains("SKLAD_PRIHODA|62d5698c", case=False, na=False))
    )

    # 4. GURUHLASH (Aggregation)
    
    # 1. Sotuv Soni va Oxirgi Sotuv Sanasi
    grp_sales = df_history[df_history['is_sale']].groupby('product_id').agg(
        Sotuv_Soni=('miqdor', 'sum'),       # <--- Jami sotilgan dona
        Last_Sale=('sana', 'max')           # <--- Oxirgi sotuv sanasi
    ).reset_index()

    # 2. Birinchi Kelgan Sana (Tovarning Yoshi)
    grp_arrival = df_history[df_history['is_arrival']].groupby('product_id').agg(
        First_Arrival=('sana', 'min')
    ).reset_index()

    # 5. ASOSIY JADVALNI BIRLASHTIRISH
    df_main = pd.merge(df_prod, grp_sales, on='product_id', how='left')
    df_main = pd.merge(df_main, grp_arrival, on='product_id', how='left')

    # NaN larni to'ldirish
    df_main['Sotuv_Soni'] = df_main['Sotuv_Soni'].fillna(0)
    
    # Faqat Qoldig'i borlarni olamiz (yoki STR ni ko'rish uchun hammasini qoldirsak bo'ladi)
    # Skidka qilish uchun baribir qoldiq kerak:
    df_main = df_main[df_main['Qoldiq'] > 0].copy()

    if df_main.empty:
        return None, "⚠️ Qoldig'i bor tovarlar topilmadi."

    # 6. HISOB-KITOB (JIMLIK, STATUS, STR)
    
    # Sanalarni to'g'irlash
    df_main['First_Arrival'] = pd.to_datetime(df_main['First_Arrival'])
    df_main['Last_Sale'] = pd.to_datetime(df_main['Last_Sale'])
    
    # Yoshi
    df_main['Yoshi'] = (TODAY - df_main['First_Arrival']).dt.days.fillna(0)
    
    # Jimlik
    df_main['Jimlik'] = (TODAY - df_main['Last_Sale']).dt.days
    df_main['Jimlik'] = df_main['Jimlik'].fillna(df_main['Yoshi']) # Sotilmagan bo'lsa Yoshi olinadi

    # Formatlash
    def fmt_price(val):
        try: return f"{int(float(val)):,}".replace(",", " ")
        except: return "0"
    
    def fmt_date(d):
        if pd.isnull(d): return "-"
        return d.strftime("%d.%m.%Y")

    analysis_data = []

    for _, row in df_main.iterrows():
        stock = int(row['Qoldiq'])
        sold = int(row['Sotuv_Soni'])
        days_quiet = int(row['Jimlik'])
        days_old = int(row['Yoshi'])
        
        # --- STR HISOBLASH ---
        # Formula: Sotuv / (Sotuv + Qoldiq) * 100
        # Bu formula "Jami aylanishdagi tovarning necha foizi sotildi" degan ma'noni beradi
        total_supply = stock + sold
        if total_supply > 0:
            str_percent = (sold / total_supply) * 100
        else:
            str_percent = 0

        # STATUS LOGIKASI
        status = "✅ Normal"
        if days_quiet > 60: 
            status = "🔴 O'LIK YUK (>60 kun)"
        elif days_quiet > 30: 
            status = "🟡 SEKIN (>30 kun)"
        elif days_old > 20 and sold == 0:
            status = "⚠️ START XATO (20 kun, 0 sotuv)"

        analysis_data.append({
            'Status': status,
            'Artikul': row.get('Артикул', ''),
            'Rang': row.get('Цвет', ''),
            'Barkod': row.get('Баркод', ''),
            'Qoldiq Soni': stock,
            'Sotuv Soni': sold,                 # <--- QO'SHILDI
            'STR (%)': round(str_percent, 1),   # <--- QO'SHILDI
            'Jimlik (Kun)': days_quiet,
            'Yoshi (Kun)': days_old,
            'Oxirgi Sotuv': fmt_date(row['Last_Sale']),
            'Kelgan Sana': fmt_date(row['First_Arrival']),
            'Sotuv Narxi': fmt_price(row.get('Sotuv_Narxi', 0)),
            'Aksiya Narxi': fmt_price(row.get('Aksiya_Narxi', 0)),
            'Tannarx': fmt_price(row.get('Tannarx', 0)),
            'Vid': row.get('Вид', ''),
            'Material': row.get('Материал', ''),
            'Postavchik': row.get('Поставщик', ''),
            'Nomi': row.get('Наименование', '')
        })

    # 7. EXCELGA Yozish
    df_final = pd.DataFrame(analysis_data)
    
    # Sortlash: Eng muhimi - Jimlik (Ko'p turganlar) va STR (Kam sotilganlar)
    df_final = df_final.sort_values(by=['Jimlik (Kun)', 'STR (%)'], ascending=[False, True])

    safe_subcat = target_subcat.replace("/", "_")
    fname = f"Skidka_Analiz_{safe_subcat}.xlsx".replace(" ", "_")

    with pd.ExcelWriter(fname, engine='openpyxl') as writer:
        df_final.to_excel(writer, index=False, sheet_name="Analiz")
        
        worksheet = writer.sheets['Analiz']
        for column_cells in worksheet.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            worksheet.column_dimensions[column_cells[0].column_letter].width = length + 2

    return fname, f"📉 <b>SKIDKA ANALIZ (Full)</b>\n\nJami: {len(df_final)} ta model\nO'rtacha STR: {round(df_final['STR (%)'].mean(), 1)}%"


def run_top_sales_analysis():
    print("🚀 TOP XITLAR ANALIZI BOSHLANDI...")

    ANALIZ_KUNLARI = 25  # Oxirgi 25 kunda kelganlar

    conn = sqlite3.connect(DB_FILE)
    try:
        # 1. Ma'lumotlarni o'qish (To'liq)
        df_hist = pd.read_sql("SELECT * FROM d_History", conn)
        df_prod = pd.read_sql("SELECT * FROM d_Mahsulotlar", conn)
    except Exception as e:
        conn.close()
        return None, f"❌ Bazada xatolik: {e}"
    conn.close()

    if df_hist.empty or df_prod.empty:
        return None, "❌ Baza bo'sh. Avval katalogni yangilang."

    # Data tayyorlash
    df_hist['sana'] = pd.to_datetime(df_hist['sana'])
    df_hist['miqdor'] = pd.to_numeric(df_hist['miqdor'], errors='coerce').fillna(0)

    # Bizning do'kon ID lari (Global o'zgaruvchidan olamiz)
    my_shop_ids = [k for k, v in SHOP_MAP.items() if v in MY_SHOPS]

    # --- A) YANGI KELGANLARNI TOPISH ---
    mask_arrival = (
        (df_hist['from_shop_id'].str.contains("62d5698c", na=False) | (df_hist['from_shop_id'] == "SKLAD_PRIHODA")) &
        (df_hist['to_shop_id'].isin(my_shop_ids))
    )
    arrivals = df_hist[mask_arrival].groupby('product_id')['sana'].min().reset_index()
    arrivals.columns = ['product_id', 'first_arrival']

    # Filtr: Oxirgi 25 kun
    TODAY = datetime.now()
    arrivals['days_old'] = (TODAY - arrivals['first_arrival']).dt.days
    target_products = arrivals[arrivals['days_old'] <= ANALIZ_KUNLARI]

    if target_products.empty:
        return None, "⚠️ Oxirgi 25 kunda yangi tovar kelmagan."

    # --- B) SOTUVLARNI HISOBLASH ---
    all_sales = df_hist[df_hist['turi'].astype(str).str.contains("sale|order|Sotuv", case=False, na=False)]
    my_sales = all_sales[all_sales['from_shop_id'].isin(my_shop_ids)]

    sales_grouped = my_sales.groupby('product_id')['miqdor'].sum().reset_index()
    sales_grouped.columns = ['product_id', 'total_sold']

    # --- C) BIRLASHTIRISH ---
    step1 = pd.merge(target_products, sales_grouped, on='product_id', how='left')
    step1['total_sold'] = step1['total_sold'].fillna(0)

    # Faqat sotuvi borlar (Top bo'lishi uchun)
    final_df = step1[step1['total_sold'] > 0].copy()

    if final_df.empty:
        return None, "⚠️ Yangi tovarlar bor, lekin hali bittasi ham sotilmagan."

    # Mahsulot info qo'shish
    full_data = pd.merge(final_df, df_prod, on='product_id', how='left')

    # --- D) HISOB-KITOB VA JADVAL ---
    result_list = []

    for idx, row in full_data.iterrows():
        sold = row['total_sold']
        current_stock = row.get('Qoldiq', 0)

        # STR Formula
        total_supply = current_stock + sold
        str_p = (sold / total_supply * 100) if total_supply > 0 else 0

        # Narx formatlash
        def fmt(val):
            try: return f"{int(float(val)):,}".replace(",", " ")
            except: return "0"

        result_list.append({
            'Artikul': row.get('Артикул', '-'),
            'Barkod': row.get('Баркод', '-'),
            'Rang': row.get('Цвет', '-'),
            'STR %': round(str_p, 1),
            'Kelgan Sana': row['first_arrival'].strftime("%d.%m"),
            'Yoshi': row['days_old'],
            'Sotildi': int(sold),
            'Qoldiq': int(current_stock),
            'Material': row.get('Материал', '-'),
            'Sotuv Narxi': fmt(row.get('Sotuv_Narxi', 0)),
            'Tannarx': fmt(row.get('Tannarx', 0)),
            'Aksiya': fmt(row.get('Aksiya_Narxi', 0)),
            'Nomi': row.get('Наименование', '-')
        })

    # Excelga yozish
    df_res = pd.DataFrame(result_list)
    df_res = df_res.sort_values(by='STR %', ascending=False)

    sana_str = TODAY.strftime("%d.%m")
    file_name = f"Top_New_{sana_str}.xlsx"
    df_res.to_excel(file_name, index=False)

    stats_msg = (
        f"🔥 <b>TOP YANGI TOVARLAR</b> (Oxirgi {ANALIZ_KUNLARI} kun)\n\n"
        f"📦 Topilgan modellar: {len(df_res)} ta\n"
        f"🚀 Eng yuqori STR: {df_res.iloc[0]['STR %']}%\n"
        f"💰 Eng ko'p sotilgan: {df_res['Sotildi'].max()} dona"
    )

    return file_name, stats_msg

def run_advanced_sales_analysis(target_subcat=None):
    print("🚀 TO'LIQ ANALIZ (Do'konlar kesimida - Aniq Hisob)...")
    TODAY = datetime.now()

    conn = sqlite3.connect(DB_FILE)
    try:
        # Tarix va Mahsulotlarni o'qish (Barcha kerakli ustunlar shu yerda)
        df_hist = pd.read_sql("SELECT * FROM d_History", conn)
        df_prod = pd.read_sql("SELECT product_id, Подкатегория, Артикул, Баркод, Цвет, Sotuv_Narxi, Tannarx, Aksiya_Narxi, Поставщик, Вид, Материал, Наименование FROM d_Mahsulotlar", conn)
    except Exception as e:
        conn.close()
        return None, f"❌ Bazada xatolik: {e}"
    conn.close()

    if df_hist.empty or df_prod.empty:
        return None, "❌ Ma'lumotlar topilmadi."

    # Filtrlash (agar kategoriya tanlangan bo'lsa)
    if target_subcat:
        df_prod = df_prod[df_prod['Подкатегория'] == target_subcat]
        relevant_ids = df_prod['product_id'].unique()
        df_hist = df_hist[df_hist['product_id'].isin(relevant_ids)]

    if df_prod.empty or df_hist.empty:
        return None, "⚠️ Bu kategoriyada ma'lumot yo'q."

    # 1. MA'LUMOTLARNI TAYYORLASH
    df_hist['from_name'] = df_hist['from_shop_id'].map(SHOP_MAP).fillna(df_hist['from_shop_id'])
    df_hist['to_name'] = df_hist['to_shop_id'].map(SHOP_MAP).fillna(df_hist['to_shop_id'])
    
    df_hist['sana'] = pd.to_datetime(df_hist['sana'])
    df_hist['miqdor'] = pd.to_numeric(df_hist['miqdor'], errors='coerce').fillna(0).abs()

    # 2. HARAKAT TURINI ANIQLASH (Kuchaytirilgan Mantiq)
    def joylashuvni_aniqlash(row):
        turi = str(row.get('turi', '')).lower()
        from_shop = str(row.get('from_name', ''))
        to_shop = str(row.get('to_name', '')) 
        
        # A) Import (Skladdan kirim)
        if 'import' in turi or 'kirim' in turi: 
             return "SKLAD_PRIHODA", to_shop
        
        # B) Sotuv (Mijozga)
        elif 'sale' in turi or 'order' in turi or 'sotuv' in turi or 'продажа' in turi:
            return from_shop, "Mijoz"
        
        # C) Vozvrat (Mijozdan do'konga)
        elif 'return' in turi or 'vozvrat' in turi or 'возврат' in turi:
            target = to_shop if to_shop and to_shop != 'nan' else from_shop
            return "Mijoz", target
        
        # D) Spisaniya (Hisobdan chiqarish)
        elif 'write' in turi or 'spisan' in turi or 'списан' in turi:
            return from_shop, "Spisaniya"
        
        # E) Transfer (Sklad yoki Boshqa do'kon)
        elif 'transfer' in turi or 'ko\'chirish' in turi or 'трансфер' in turi:
            if not from_shop or from_shop in ["SKLAD_PRIHODA", "СКЛАД ПРИХОДА", "None", "nan"]:
                return "SKLAD_PRIHODA", to_shop
            return from_shop, to_shop
            
        else:
            return from_shop, to_shop

    print("⚙️ Harakatlar aniqlanmoqda...")
    df_hist[['Qayerdan', 'Qayerga']] = df_hist.apply(lambda row: pd.Series(joylashuvni_aniqlash(row)), axis=1)

    my_shop_names = MY_SHOPS 

    # ---------------------------------------------------------
    # 3. KIRIMLARNI HISOBLASH (INFLOW)
    # ---------------------------------------------------------
    mask_in = df_hist['Qayerga'].isin(my_shop_names)
    df_in = df_hist[mask_in].copy()
    
    df_in['is_import'] = df_in['Qayerdan'] == "SKLAD_PRIHODA"
    df_in['is_vozvrat'] = df_in['Qayerdan'] == "Mijoz"
    
    grp_in = df_in.groupby(['product_id', 'Qayerga']).agg(
        Skladdan_Kelgan=('miqdor', lambda x: x[df_in.loc[x.index, 'is_import']].sum()),
        Vozvrat_Kirim=('miqdor', lambda x: x[df_in.loc[x.index, 'is_vozvrat']].sum()),
        Transfer_Kirim=('miqdor', lambda x: x[~df_in.loc[x.index, 'is_import'] & ~df_in.loc[x.index, 'is_vozvrat']].sum()),
        First_Arrival=('sana', 'min'),
        Last_In_Date=('sana', 'max')
    ).reset_index().rename(columns={'Qayerga': 'Dokon'})

    # ---------------------------------------------------------
    # 4. CHIQIMLARNI HISOBLASH (OUTFLOW)
    # ---------------------------------------------------------
    mask_out = df_hist['Qayerdan'].isin(my_shop_names)
    df_out = df_hist[mask_out].copy()

    df_out['is_sale'] = df_out['Qayerga'] == "Mijoz"
    df_out['is_spisaniya'] = df_out['Qayerga'] == "Spisaniya"

    grp_out = df_out.groupby(['product_id', 'Qayerdan']).agg(
        Sotuv=('miqdor', lambda x: x[df_out.loc[x.index, 'is_sale']].sum()),
        Spisaniya=('miqdor', lambda x: x[df_out.loc[x.index, 'is_spisaniya']].sum()),
        Transfer_Chiqim=('miqdor', lambda x: x[~df_out.loc[x.index, 'is_sale'] & ~df_out.loc[x.index, 'is_spisaniya']].sum()),
        Last_Sale_Date=('sana', lambda x: x[df_out.loc[x.index, 'is_sale']].max())
    ).reset_index().rename(columns={'Qayerdan': 'Dokon'})

    # 5. BIRLASHTIRISH VA QOLDIQ
    df_final = pd.merge(grp_in, grp_out, on=['product_id', 'Dokon'], how='outer').fillna(0)

    df_final['Jami_Kirim'] = df_final['Skladdan_Kelgan'] + df_final['Vozvrat_Kirim'] + df_final['Transfer_Kirim']
    df_final['Jami_Chiqim'] = df_final['Sotuv'] + df_final['Spisaniya'] + df_final['Transfer_Chiqim']
    df_final['Haqiqiy_Qoldiq'] = (df_final['Jami_Kirim'] - df_final['Jami_Chiqim']).clip(lower=0)

    # 6. PRODUCT INFO QO'SHISH
    df_merged = pd.merge(df_final, df_prod, on='product_id', how='left')

    # Faqat aktivlarni qoldiramiz
    df_merged = df_merged[(df_merged['Haqiqiy_Qoldiq'] > 0) | (df_merged['Sotuv'] > 0)]

    df_merged['First_Arrival'] = pd.to_datetime(df_merged['First_Arrival'])
    df_merged['Yoshi'] = (TODAY - df_merged['First_Arrival']).dt.days
    df_merged['Yoshi'] = df_merged['Yoshi'].apply(lambda x: x if x > 0 else 1)
    
    df_merged['STR %'] = (df_merged['Sotuv'] / df_merged['Jami_Kirim'].replace(0, 1)) * 100

    def fmt_price(val):
        try: return f"{int(float(val)):,}".replace(",", " ")
        except: return "0"
    
    def fmt_date(d):
        if pd.isnull(d) or d == 0: return "-"
        try: return pd.to_datetime(d).strftime("%d.%m.%Y")
        except: return "-"

    result_rows = []
    for _, row in df_merged.iterrows():
        status = ""
        stock = row['Haqiqiy_Qoldiq']
        str_val = row['STR %']
        
        if stock == 0 and str_val > 50: status = "🔴 YO'QOTILGAN SAVDO"
        elif stock > 5 and str_val < 10: status = "🟡 O'LIK YUK"
        elif str_val >= 80: status = "🔥 BESTSELLER"

        # --------------------------------------------------------
        # 🔥 O'ZGARISH: Aksiya, Vid, Material, Postavchik qo'shildi
        # --------------------------------------------------------
        result_rows.append({
            'Status': status,
            'Do\'kon': row['Dokon'],
            'Podkategoriya': row.get('Подкатегория', ''),
            'Artikul': row.get('Артикул', ''),
            'Rang': row.get('Цвет', ''),
            'Barkod': row.get('Баркод', ''),
            'Vid': row.get('Вид', ''),          # <--- QO'SHILDI
            'Material': row.get('Материал', ''),# <--- QO'SHILDI
            'Postavchik': row.get('Поставщик', ''), # <--- QO'SHILDI
            'Qoldiq': int(stock),
            'Sotuv': int(row['Sotuv']),
            'STR (%)': round(str_val, 1),
            'Spisaniya': int(row['Spisaniya']),
            'Skladdan Kelgan': int(row['Skladdan_Kelgan']),
            'Transfer Kirim': int(row['Transfer_Kirim']),
            'Transfer Chiqim': int(row['Transfer_Chiqim']),
            'Sotuv Narxi': fmt_price(row.get('Sotuv_Narxi', 0)),
            'Aksiya Narxi': fmt_price(row.get('Aksiya_Narxi', 0)), # <--- QO'SHILDI
            'Tannarx': fmt_price(row.get('Tannarx', 0)),
            'Yoshi': int(row['Yoshi']),
            'Kelgan Sana': fmt_date(row['First_Arrival']),
            'Oxirgi Sotuv': fmt_date(row['Last_Sale_Date']),
            'Nom': row.get('Наименование', '')
        })

    if not result_rows:
        return None, "⚠️ Hisob-kitob bo'yicha natija chiqmadi."

    df_res = pd.DataFrame(result_rows)
    # Sortlash: Do'kon -> Artikul
    df_res = df_res.sort_values(by=['Do\'kon', 'Artikul', 'STR (%)'], ascending=[True, True, False])

    sana_str = TODAY.strftime("%d.%m")
    safe_subcat = target_subcat.replace('/', '_') if target_subcat else "All"
    file_name = f"Analiz_Dokonlar_{safe_subcat}_{sana_str}.xlsx"

    with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
        df_res.to_excel(writer, index=False, sheet_name="Do'konlar Kesimida")
        ws = writer.sheets["Do'konlar Kesimida"]
        for column_cells in ws.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            ws.column_dimensions[column_cells[0].column_letter].width = length + 2

    return file_name, f"📊 <b>DO'KONLAR KESIMIDA (TO'LIQ)</b>\n\nJami: {len(df_res)} ta qator."

def init_cache_table():
    """Kesh jadvalini yaratish (birinchi ishga tushganda)"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache_metadata (
            podkategoriya TEXT PRIMARY KEY,
            last_updated DATE NOT NULL
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ cache_metadata jadvali tayyor.")


def is_cache_valid(podkat: str) -> bool:
    """
    Bugungi kesh bormi tekshiradi.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT last_updated FROM cache_metadata 
            WHERE podkategoriya = ?
        """, (podkat,))
        
        result = cursor.fetchone()
        
        if not result:
            print(f"⚠️ '{podkat}' uchun kesh topilmadi.")
            return False
        
        last_date = datetime.strptime(result[0], "%Y-%m-%d").date()
        today = datetime.now().date()
        
        if last_date == today:
            print(f"✅ '{podkat}' bugungi kesh mavjud ({last_date})")
            return True
        else:
            print(f"⏰ '{podkat}' keshi eski ({last_date}). Yangilash kerak.")
            return False
            
    except Exception as e:
        print(f"❌ Kesh tekshirishda xato: {e}")
        return False
    finally:
        conn.close()


def update_cache_metadata(podkat: str):
    """
    Kesh metadata ni yangilaydi (bugungi sana bilan).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    cursor.execute("""
        INSERT OR REPLACE INTO cache_metadata (podkategoriya, last_updated)
        VALUES (?, ?)
    """, (podkat, today))
    
    conn.commit()
    conn.close()
    print(f"✅ Kesh yangilandi: '{podkat}' → {today}")


def clear_category_from_db(podkat: str):
    """
    Eski kategoriya ma'lumotlarini bazadan o'chiradi.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Product ID larni olamiz
    cursor.execute("SELECT product_id FROM d_Mahsulotlar WHERE Подкатегория = ?", (podkat,))
    product_ids = [row[0] for row in cursor.fetchall()]
    
    # Mahsulotlarni o'chirish
    cursor.execute("DELETE FROM d_Mahsulotlar WHERE Подкатегория = ?", (podkat,))
    deleted_products = cursor.rowcount
    
    # Tarixni o'chirish
    if product_ids:
        placeholders = ','.join('?' * len(product_ids))
        cursor.execute(f"DELETE FROM d_History WHERE product_id IN ({placeholders})", product_ids)
        deleted_history = cursor.rowcount
    else:
        deleted_history = 0
    
    conn.commit()
    conn.close()
    
    print(f"🗑️ '{podkat}' eski ma'lumotlari o'chirildi:")
    print(f"   - Mahsulotlar: {deleted_products} ta")
    print(f"   - Tarix: {deleted_history} ta")