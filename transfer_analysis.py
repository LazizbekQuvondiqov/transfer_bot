
import pandas as pd
import numpy as np
from datetime import datetime
import random
import sqlite3
from config import DB_FILE



import requests
import pandas as pd
import sqlite3
import time
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import (
    BROWSER_TOKEN,
    PLATFORM_ID,
    COOKIE_VALUE,
    SECRET_KEY,
    ADMIN_BASE,
    HISTORY_BASE_URL,
    DB_FILE
)


MAX_WORKERS = 25 # Optimal tezlik

def get_session():
    s = requests.Session()
    # Xato bersa qayta urinish (Retry)
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
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

def fetch_all_products_fresh(token):
    all_items = []
    page = 1
    per_page = 900
    print("📦 Catalog yuklanmoqda...")
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
            print(f"   📄 Sahifa {page}: {len(data)} ta mahsulot.")
            if len(data) < per_page: break
            page += 1
        except Exception as e:
            print(f"❌ Catalog yuklashda xato: {e}")
            break
    return all_items

def fetch_history_single(product_id):
    url = f"{HISTORY_BASE_URL}/api/v2/product-movement/{product_id}"

    headers = {
        "authorization": BROWSER_TOKEN,
        "platform-id": PLATFORM_ID,
        "cookie": COOKIE_VALUE,
        "accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    clean_rows = []
    page = 1  # 1-sahifadan boshlaymiz

    while True:
        params = {"limit": 100, "page": page}  # Har safar page o'zgaradi
        try:
            r = session.get(url, params=params, headers=headers, timeout=20)
            if r.status_code == 200:
                data = r.json()
                movements = data.get('movements') or []

                # --- ENG MUHIM JOYI: Agar ro'yxat bo'sh kelsa, demak tarix tugadi ---
                if not movements:
                    break

                for move in movements:
                    clean_rows.append({
                        'product_id': product_id,
                        'sana': move.get('created_at', ''),
                        'turi': move.get('type', ''),
                        'miqdor': move.get('measurement_value', 0),
                        'from_shop_id': move.get('from_shop', ''),
                        'to_shop_id': move.get('to_shop', '')
                    })

                # Keyingi sahifaga o'tamiz
                page += 1
            else:
                # Agar server xato bersa (404, 500) tsiklni to'xtatamiz
                break
        except Exception:
            # Internet uzilsa yoki boshqa xato bo'lsa to'xtatamiz
            break

    return clean_rows

# --- transfer_analysis.py (ESKI KODNING O'RNIGA) ---

def update_db_by_category(target_category):
    """
    Faqat tanlangan kategoriya bo'yicha TARIXNI yangilaydi.
    Katalog d_Mahsulotlar jadvalidan o'qiladi (API dan emas).
    """
    from logic import is_cache_valid, update_cache_metadata
    
    # ✅ 0. KESHNI TEKSHIRISH
    if is_cache_valid(target_category):
        print(f"✅ '{target_category}' bugungi kesh mavjud. API chaqirilmaydi.")
        return True, f"✅ '{target_category}' ma'lumotlari bugun yangilangan (keshdan olindi)."
    
    print(f"🔄 '{target_category}' uchun tarix yangilanmoqda...")
    
    # ✅ 1. BAZADAN PRODUCT_ID LARNI OLISH
    conn = sqlite3.connect(DB_FILE)
    
    try:
        df_products = pd.read_sql(
            "SELECT product_id FROM d_Mahsulotlar WHERE Подкатегория = ?",
            conn,
            params=(target_category,)
        )
    except Exception as e:
        conn.close()
        return False, f"❌ Baza xatosi: {e}"
    
    if df_products.empty:
        conn.close()
        return False, f"❌ '{target_category}' bo'yicha tovar topilmadi. Avval 'Katalogni Yangilash' tugmasini bosing."
    
    product_ids = df_products['product_id'].tolist()
    print(f"📦 {len(product_ids)} ta tovar uchun tarix yuklanmoqda...")
    
    # ✅ 2. ESKI TARIXNI O'CHIRISH (Faqat shu kategoriya uchun)
    try:
        placeholders = ','.join('?' * len(product_ids))
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM d_History WHERE product_id IN ({placeholders})",
            product_ids
        )
        deleted_count = cursor.rowcount
        conn.commit()
        print(f"🗑️ Eski tarix o'chirildi: {deleted_count} ta")
    except Exception as e:
        conn.close()
        return False, f"❌ Tarixni o'chirishda xato: {e}"
    
    # ✅ 3. YANGI TARIXNI YUKLASH (Parallel)
    all_history = []
    print(f"⏳ Tarix tortilmoqda ({len(product_ids)} ta tovar)...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_pid = {
            executor.submit(fetch_history_single, pid): pid 
            for pid in product_ids
        }
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_pid):
            data = future.result()
            if data:
                all_history.extend(data)
            
            completed += 1
            if completed % 100 == 0:
                print(f"   ⏳ Progress: {completed}/{len(product_ids)}")
    
    # ✅ 4. TARIXNI BAZAGA YOZISH
    if all_history:
        df_history = pd.DataFrame(all_history)
        df_history.to_sql("d_History", conn, if_exists="append", index=False)
        msg = f"✅ '{target_category}' tarixi yangilandi! {len(product_ids)} ta tovar, {len(df_history)} ta harakat."
    else:
        msg = f"⚠️ '{target_category}': {len(product_ids)} ta tovar bor, lekin tarixi yo'q."
    
    conn.close()
    
    # ✅ 5. KESH METADATA NI YANGILASH
    update_cache_metadata(target_category)
    
    return True, msg

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
        "c91a913b-c295-4775-a7a8-4a0ce2578fa0":"СКЛАД БРАКА",
        "559bfd04-be37-4a9f-ab5b-3af44ccf524d":"Aziz"

    }



MY_SHOPS = ['ANDALUS', 'BERUNIY MEN', 'Dressco Integro', 'MAGNIT MEN', 'SHAXRISTON']

def run_transfer_analysis(
    FILE_NAME,
    TARGET_PODKAT,
    LIDER_LIMIT=50,
    MIN_DAYS_ON_SHELF=13,
    TARGET_SOLO=14
):




    print("⏳ Bazadan ma'lumotlar o'qilmoqda...")
    conn = sqlite3.connect(DB_FILE)

    df_history = pd.read_sql("SELECT * FROM d_History", conn)

    query_products = """
    SELECT
        product_id,
        Наименование,
        Артикул,
        Баркод,
        Подкатегория,
        Цвет,
        import_date,
        Поставщик,
        Qoldiq
    FROM d_Mahsulotlar
    """

    try:
        df_products = pd.read_sql(query_products, conn)
        print("✅ Productlar o'qildi.")
    except Exception as e:
        print(f"❌ Xatolik: {e}")
        conn.close()
        return


    conn.close()

    # 2. BIRLASHTIRISH
    df_full = pd.merge(df_history, df_products, on='product_id', how='left')



    df_full['from_name'] = df_full['from_shop_id'].map(SHOP_MAP).fillna(df_full['from_shop_id'])
    df_full['to_name'] = df_full['to_shop_id'].map(SHOP_MAP).fillna(df_full['to_shop_id'])

    # 4. JOYLASHUV MANTIQI (TUZATILDI)
    def joylashuvni_aniqlash(row):
        turi = str(row.get('turi', '')).lower()
        from_shop = row.get('from_name', '')
        to_shop = row.get('to_name', '')
        postavshik = row.get('Поставщик', 'Noma\'lum')

        # A) IMPORT (Postavshik -> Do'kon/Sklad)
        if 'import' in turi:
            # Importda "Qayerdan" bu Postavshik bo'ladi
            return f"Postavshik: {postavshik}", to_shop

        # B) SOTUV (Do'kon -> Mijoz)
        elif 'sale' in turi or 'order' in turi or 'sotuv' in turi:
            return from_shop, "Mijozga 👤"

        # C) VOZVRAT (Mijoz -> Do'kon)
        elif 'return' in turi or 'vozvrat' in turi:
            # Qaysi do'konga qaytgan bo'lsa o'sha yoziladi
            target = to_shop if to_shop else from_shop
            return "Mijozdan 👤", target

        # D) TRANSFER (Sklad -> Do'kon yoki Do'kon -> Do'kon)
        elif 'transfer' in turi:
            # Agar qayerdan chiqqani noma'lum bo'lsa yoki Sklad ID bo'lsa -> SKLAD_PRIHODA deymiz
            # Bu keyingi kodda "Skladdan_Kelgan" ustunini hisoblash uchun juda muhim
            if not from_shop or from_shop == "SKLAD_PRIHODA":
                return "SKLAD_PRIHODA", to_shop
            return from_shop, to_shop

        # E) BOSHQA HOLATLAR
        else:
            return from_shop, to_shop

    print("⚙️ Joylashuv aniqlanmoqda...")

# TUZATILGAN VARIANT: result_type='expand' ishlatish shart
    joylashuv_df = df_full.apply(joylashuvni_aniqlash, axis=1, result_type='expand')
    df_full['Qayerdan'] = joylashuv_df[0]
    df_full['Qayerga'] = joylashuv_df[1]

    # 5. TARJIMA
    tarjima = {
        'import': 'Kirim (Import)',
        'order': 'Sotuv',
        'sale': 'Sotuv',
        'transfer': 'Ko\'chirish (Transfer)',
        'return': 'Vozvrat (Qaytarish)',
        'write-off': 'Spisaniya (Hisobdan chiqarish)',
        'repricing': 'Narx o\'zgarishi',
        'stocktaking': 'Inventarizatsiya'
    }
    df_full['turi'] = df_full['turi'].map(tarjima).fillna(df_full['turi'])

    # 6. USTUNLARNI TANLASH VA FILTRLASH
    cols = [
        'Qoldiq', 'sana', 'turi', 'miqdor', 'Наименование', 'Артикул',
        'Баркод', 'Подкатегория', 'Цвет', 'import_date', 'Qayerdan', 'Qayerga'
    ]

    available_cols = [c for c in cols if c in df_full.columns]
    df_final = df_full[available_cols]

    # Keraksiz narsalarni o'chirish
    keraksiz = ["Aziz", "Marketing", "Parfume"]
    df_final = df_final[
        ~df_final['Qayerdan'].isin(keraksiz) &
        ~df_final['Qayerga'].isin(keraksiz) &
        ~df_final['Qayerdan'].str.contains("Parfume", na=False)
    ]

    # Maxsus filtrlar
    df_final = df_final[~df_final['Артикул'].astype(str).str.startswith("100", na=False)]
    df_final = df_final[~df_final['Артикул'].astype(str).str.startswith("0", na=False)]
    df_final = df_final[~df_final['Наименование'].astype(str).str.startswith("К-т", na=False)]

    # 7. SAQLASH
    file_name = "Billz_Tarix_Batafsil.csv"
    df_final.to_csv(file_name, index=False, encoding='utf-8-sig')

    print(f"✅ TAYYOR! CSV formatida saqlandi: '{file_name}'")


    TODAY = datetime.now()
    # main(selected_categories=['Американка','Рубашка с дл/р','Куртка','К-т Верх','Кардиган','Свитер','Кроссовки'])


    print(f"📂 '{FILE_NAME}' o‘qilmoqda...")
    try:
        df = pd.read_csv(FILE_NAME, low_memory=False)
    except FileNotFoundError:
        print("❌ Fayl topilmadi!")
        return


    df['sana'] = pd.to_datetime(df['sana'], errors='coerce')

    # === OXIRGI KIRIM SANASI (HAMMA TUR) ===
    mask_all_in = (
        df['Qayerga'].isin(MY_SHOPS) &
        ~df['turi'].astype(str).str.contains(
            "Spisaniya|Hisobdan|Write-off|Sotuv|Sale|Продажа",
            case=False, na=False
        )
    )




    # ID -> Nom o'girish
    df['Qayerdan'] = df['Qayerdan'].map(SHOP_MAP).fillna(df['Qayerdan'])
    df['Qayerga']  = df['Qayerga'].map(SHOP_MAP).fillna(df['Qayerga'])

    # Keraksiz qatorlarni tozalash (Narx o'zgarishi)
    df = df[~df['turi'].astype(str).str.contains("Narx|Переоценка|Repricing", case=False, na=False)]
    df['miqdor'] = pd.to_numeric(df['miqdor'], errors='coerce').fillna(0)

    # Kategoriya filtri
    df = df[df['Подкатегория'] == TARGET_PODKAT].copy()
    df['Цвет'] = df['Цвет'].fillna('-')

    print(f"✅ Analiz ketmoqda: {len(df)} qator...")

    # =========================================================
    # 3. MANTIQ: SKLAD -> DO'KON (REAL IMPORT)
    # =========================================================

    mask_real_import = (df['Qayerdan'] == 'SKLAD_PRIHODA') & (df['Qayerga'].isin(MY_SHOPS))
    df_import = df[mask_real_import].groupby(['Артикул','Цвет','Qayerga'])['miqdor'].sum().reset_index().rename(columns={'Qayerga':'Dokon', 'miqdor':'Skladdan_Kelgan'})

    # --- 2. TRANSFER KIRIM (DO'KONDAN DO'KONGA) ---

    mask_trans_in = (df['Qayerdan'].isin(MY_SHOPS)) & \
                    (df['Qayerga'].isin(MY_SHOPS)) & \
                    (~df['turi'].astype(str).str.contains("Spisaniya|Hisobdan|Write-off", case=False, na=False))

    df_trans_in = df[mask_trans_in].groupby(['Артикул','Цвет','Qayerga'])['miqdor'].sum().reset_index().rename(columns={'Qayerga':'Dokon', 'miqdor':'Transfer_Kirim'})


    mask_voz = (df['Qayerga'].isin(MY_SHOPS)) & (
        df['turi'].astype(str).str.contains("Vozvrat|Return", case=False) |
        df['Qayerdan'].str.contains("Mijoz", case=False)
    )
    df_voz = df[mask_voz].groupby(['Артикул','Цвет','Qayerga'])['miqdor'].sum().reset_index().rename(columns={'Qayerga':'Dokon', 'miqdor':'Vozvrat_Kirim'})

    # --- 4. CHIQIMLAR (SOTUV + TRANSFER + BRAK + SKLADGA QAYTISH) ---
    # SHART: Qayerdan = BIZNING DO'KON (Nima bo'lishidan qat'iy nazar)
    mask_out = df['Qayerdan'].isin(MY_SHOPS)
    df_out = df[mask_out].groupby(['Артикул','Цвет','Qayerdan'])['miqdor'].sum().reset_index().rename(columns={'Qayerdan':'Dokon', 'miqdor':'Jami_Chiqim'})

    # --- 5. SOTUV (Faqat Foiz uchun) ---
    mask_sale = mask_out & df['turi'].astype(str).str.contains("Sotuv|Sale|Продажа", case=False)
    df_sale = df[mask_sale].groupby(['Артикул','Цвет','Qayerdan'])['miqdor'].sum().reset_index().rename(columns={'Qayerdan':'Dokon', 'miqdor':'Sotuv'})

    # --- 6. SANALAR ---

    s_last_in = df[mask_all_in].groupby(
        ['Артикул','Цвет','Qayerga']
    )['sana'].max().reset_index().rename(
        columns={'Qayerga':'Dokon', 'sana':'Last_In'}
    )



    s_last_sale = df[mask_sale].groupby(['Артикул','Цвет','Qayerdan'])['sana'].max().reset_index().rename(columns={'Qayerdan':'Dokon', 'sana':'Last_Sale'})
    # Do'konga birinchi marta aynan SKLADDAN kelgan sanani olamiz (tovar yoshi uchun)
    s_first_arrival = df[mask_real_import].groupby(['Артикул','Цвет','Qayerga'])['sana'].min().reset_index().rename(columns={'Qayerga':'Dokon', 'sana':'First_Arrival'})

    # --- BIRLASHTIRISH ---



    df_stats = pd.merge(df_import, df_trans_in, on=['Артикул','Цвет','Dokon'], how='outer')
    df_stats = pd.merge(df_stats, df_voz, on=['Артикул','Цвет','Dokon'], how='outer')
    df_stats = pd.merge(df_stats, df_out, on=['Артикул','Цвет','Dokon'], how='outer')
    df_stats = pd.merge(df_stats, df_sale, on=['Артикул','Цвет','Dokon'], how='outer')
    df_stats = pd.merge(df_stats, s_last_sale, on=['Артикул','Цвет','Dokon'], how='left')
    df_stats = pd.merge(df_stats, s_first_arrival, on=['Артикул','Цвет','Dokon'], how='left')

    df_stats = pd.merge(df_stats, s_last_in,
                        on=['Артикул','Цвет','Dokon'], how='left')

    df_stats['Last_In'] = pd.to_datetime(df_stats['Last_In']).fillna(TODAY)
    df_stats['Days_From_Last_In'] = (TODAY - df_stats['Last_In']).dt.days

    df_stats = df_stats.fillna(0)
    df_stats['Last_Sale'] = pd.to_datetime(df_stats['Last_Sale']).replace(0, pd.Timestamp("2000-01-01"))
    df_stats['First_Arrival'] = pd.to_datetime(df_stats['First_Arrival']).replace(0, pd.Timestamp.now())


    # 1. JAMI KIRIMNI YIG'AMIZ (Maxraj va Qoldiq uchun)
    df_stats['Jami_Kirim_Summasi'] = (
        df_stats['Skladdan_Kelgan'] +
        df_stats['Transfer_Kirim'] +
        df_stats['Vozvrat_Kirim']
    )


    # Jami Kirim - Jami Chiqim (Sotuv va Transfer Out ichida bor)
    df_stats['Hisoblangan_Qoldiq'] = (df_stats['Jami_Kirim_Summasi'] - df_stats['Jami_Chiqim']).clip(lower=0)

    # 3. KUN
    df_stats['Days_On_Shelf'] = (TODAY - df_stats['First_Arrival']).dt.days

    # =========================================================
    # TO'G'RI FOIZ FORMULASI
    # =========================================================
    # Maxraj = Do'konga kirgan jami tovarlar (Sklad + Transfer + Vozvrat)
    df_stats['Maxraj'] = df_stats['Jami_Kirim_Summasi'].replace(0, 1)

    # Formula: (Sotuv / Jami Kirim) * 100
    df_stats['Foiz'] = (df_stats['Sotuv'] / df_stats['Maxraj']) * 100

    # Faqat aktiv pozitsiyalar
    df_stats = df_stats[(df_stats['Hisoblangan_Qoldiq'] > 0) | (df_stats['Sotuv'] > 0)]

    print(f"📊 Hisob-kitob tayyor. Foiz va Qoldiq formulasi to'g'rilandi.")
    # =========================================================
    # YANGI QISM: DO'KONLAR ARO AYLANISH TARIXI (STATUS)
    # =========================================================
    print("🕵️‍♀️ Tovar 'Sayyohligi' aniqlanmoqda (Faqat Do'kon -> Do'kon)...")

    visited_map = {}

    # 1. Faqat Do'kondan-Do'konga bo'lgan harakatlarni filtrlaymiz
    mask_only_shop_transfer = (df['Qayerdan'].isin(MY_SHOPS)) & (df['Qayerga'].isin(MY_SHOPS))
    df_movements = df[mask_only_shop_transfer]

    # 2. Har bir tovar bo'yicha hisoblaymiz
    for (art, color), group in df_movements.groupby(['Артикул', 'Цвет']):
        # Bu operatsiyalarda qatnashgan barcha do'konlar (Jo'natuvchi va Qabul qiluvchi)
        shops_involved = set(group['Qayerdan'].unique()).union(set(group['Qayerga'].unique()))
        
        # Nechta unique do'konda aylangan?
        count = len(shops_involved)
        total_shops = len(MY_SHOPS)
        
        # Status matnini yasaymiz
        if count == total_shops:
            status_text = f"❗ FULL: {count}/{total_shops} (Hammasini aylanib chiqdi)"
        elif count >= 3:
            status_text = f"⚠️ ACTIVE: {count}/{total_shops} ta do'konda aylandi"
        else:
            status_text = f"START: {count}/{total_shops} (Kam aylangan)"
            
        visited_map[(art, color)] = status_text

    print("✅ Tarixiy statuslar tayyorlandi.")

    transfer_list = []
    skipped_list = []
    summary_list = []

    row_summary = {'Kategoriya': TARGET_PODKAT}
    for shop in MY_SHOPS:
        shop_stock = df_stats[df_stats['Dokon'] == shop]['Hisoblangan_Qoldiq'].sum()
        row_summary[f"{shop}_Hozirgi_Qoldiq"] = int(shop_stock)
        row_summary[f"{shop}_Transfer_Harakat"] = 0

    print("⚙️ Taqsimlash...")

    for (art, color), group in df_stats.groupby(['Артикул','Цвет']):

        # Do'kon infosi
        shop_info = {}
        for shop in MY_SHOPS:
            row = group[group['Dokon'] == shop]
            if not row.empty:
                vals = row.iloc[0]

                info_str = f"Imp:{int(vals['Skladdan_Kelgan'])} | Q:{int(vals['Hisoblangan_Qoldiq'])} | S:{int(vals['Sotuv'])}"
                shop_info[f"{shop}_Info"] = info_str
            else:
                shop_info[f"{shop}_Info"] = "Imp:0 | Q:0 | S:0"

        liders = group[group['Foiz'] > LIDER_LIMIT].sort_values(['Foiz', 'Sotuv', 'Last_Sale'], ascending=[False, False, False])
        donors = group[(group['Foiz'] <= LIDER_LIMIT) & (group['Hisoblangan_Qoldiq'] > 0)].sort_values(['Foiz', 'Sotuv', 'First_Arrival'], ascending=[True, True, True])

        num_liders = len(liders)
        num_donors = len(donors)

        actions = []

        # Jami aktiv do'konlar soni
        total_active = num_liders + num_donors

        # ---------------------------------------------------------
        # 0. YANGI SHART: AGAR TOVAR FAQAT 1 TA DO'KONDA BO'LSA (SOLO)
        # ---------------------------------------------------------
        if total_active == 1:
            # O'sha yagona do'konni aniqlaymiz
            if num_liders == 1:
                current = liders.iloc[0]
                is_leader = True
            else:
                current = donors.iloc[0]
                is_leader = False

            # Random tanlash uchun nomzodlar (O'zidan boshqa barcha do'konlar)
            nomzodlar = [s for s in MY_SHOPS if s != current['Dokon']]

            if nomzodlar:
                random_shop = random.choice(nomzodlar) # <--- RANDOM TANLASH

                # A) AGAR LIDER BO'LSA (14 kun qoidasi)
                if is_leader:
                    # Oxirgi sotuvdan beri necha kun o'tdi?
                    days_quiet = (TODAY - current['Last_Sale']).days


                    if days_quiet >= TARGET_SOLO:
                        actions.append({
                            'snd': current,
                            'rcv': {'Dokon': random_shop, 'Foiz': 0},
                            'why': f"Solo Lider: 14 kundan beri jim ({days_quiet} kun). Random -> {random_shop}"
                        })

                # B) AGAR DONOR BO'LSA (Rotatsiya)
                else:
                    # 8 kunlik tekshiruv pastda baribir bor.
                    actions.append({
                        'snd': current,
                        'rcv': {'Dokon': random_shop, 'Foiz': 0},
                        'why': f"Solo Donor: Rotatsiya (Random) -> {random_shop}"
                    })

        # 1. 5 TA LIDER
        elif num_liders >= 5:
            actions.append({'snd': liders.iloc[-1], 'rcv': liders.iloc[0], 'why': "5-Shart: Sust Lider -> Kuchli Lider"})

        # 2. STANDART (Donor -> Lider) 1 ga 1
        elif num_liders > 0 and num_donors > 0:
            count = min(num_liders, num_donors)
            for i in range(count):
                actions.append({'snd': donors.iloc[i], 'rcv': liders.iloc[i], 'why': f"{num_liders}-Shart: Donor -> Lider"})


    # 3. HAMMASI LIDER, DONOR YO'Q (TUZATILGAN VERSIYA)
        elif num_liders >= 2 and num_donors == 0:
            best = liders.iloc[0]   # Eng zo'ri (Andalus)

            # MUHIM O'ZGARISH:
            # Yomonini tanlashda faqat Qoldig'i borlarni (Real donorlarni) saralaymiz
            real_liders_with_stock = liders[liders['Hisoblangan_Qoldiq'] > 0]

            if not real_liders_with_stock.empty:
                # Ro'yxatning eng oxirida turgani (Nisbatan eng kuchsizi)
                worst = real_liders_with_stock.iloc[-1]

                # O'zidan o'ziga bermasligi kerak
                if best['Dokon'] != worst['Dokon']:
                    actions.append({
                        'snd': worst,
                        'rcv': best,
                        'why': f"6-Shart: Liderlar jangi. {worst['Dokon']} (Q:{int(worst['Hisoblangan_Qoldiq'])}) -> {best['Dokon']}"
                    })

    # 4. ROTATSIYA (Lider yo'q, hamma Donor)
        elif num_liders == 0 and num_donors > 0:
            # Hozir ushbu tovar bor do'konlar ro'yxati
            mavjud = group['Dokon'].tolist()

            # Bu tovar UMUMAN YO'Q bo'lgan do'konlarni topamiz
            yangi_joylar = [d for d in MY_SHOPS if d not in mavjud]

            # AGAR yangi (bo'sh) joy topilsa -> O'sha yerga Random qilib beramiz
            if yangi_joylar:
                target_shop = random.choice(yangi_joylar) # <--- RANDOM (Faqat yo'q joydan tanlaydi)

                actions.append({
                    'snd': donors.iloc[0],
                    'rcv': {'Dokon': target_shop, 'Foiz': 0},
                    'why': f"Rotatsiya: Lider yo'q. Random yangi joyga -> {target_shop}"
                })

    # -----------------------------------------------------------
        # YANGI KOD - SHUNI O'RNIGA QO'YING
        # -----------------------------------------------------------
        
        # 1. Agar hech qanday reja (action) tuzilmagan bo'lsa
        if not actions:
            skipped_list.append({
                'Артикул': art,
                'Цвет': color,
                'Sabab': "Strategiya mos kelmadi",
                'Debug': f"Lider: {num_liders}, Donor: {num_donors} (Shartlarga tushmadi)",
                **shop_info
            })
            continue  # Keyingi tovarga o'tamiz

    # ... (Actions aniqlangandan keyin) ...
        
        # A) STATUSNI O'QIYMIZ (Agar mapda yo'q bo'lsa, demak transfer bo'lmagan)
        tovar_status = visited_map.get((art, color), "0 (Statik/Yangi)")

        done = False
        for act in actions:
            snd, rcv = act['snd'], act['rcv']
            rcv_name = rcv['Dokon'] if isinstance(rcv, pd.Series) else rcv['Dokon']

            if snd['Days_From_Last_In'] >= MIN_DAYS_ON_SHELF and snd['Hisoblangan_Qoldiq'] > 0:
                qty = snd['Hisoblangan_Qoldiq']
                row = {
                    'Артикул': art, 
                    'Цвет': color, 
                    'Qayerdan': snd['Dokon'], 
                    'Qayerga': rcv_name,
                    'Soni': qty, 
                    'Sabab': act['why'], 
                    'Status_Tarix': tovar_status,  # <--- YANGI USTUN
                    'Donor_Foiz': snd['Foiz'],
                    'Debug': f"Days={snd['Days_From_Last_In']} | Qoldiq={qty}"
                }
                # ... (davomi o'sha-o'sha)
                row.update(shop_info)
                transfer_list.append(row)
                
                # Statistikani yangilash
                row_summary[f"{snd['Dokon']}_Transfer_Harakat"] -= qty
                row_summary[f"{rcv_name}_Transfer_Harakat"] += qty
                
                done = True
                break  # Bitta transfer yetadi

        # 3. Agar reja bor, lekin shartlar (Vaqt/Qoldiq) to'g'ri kelmagan bo'lsa
        if not done:
            # Birinchi rejadagi 'snd' ma'lumotlarini olamiz (Xato bermasligi uchun)
            failed_act = actions[0]
            failed_snd = failed_act['snd']
            
            skipped_list.append({
                'Артикул': art,
                'Цвет': color,
                'Sabab': "Vaqt yoki Qoldiq yetmadi",
                'Debug': f"Reja: {failed_act['why']} | Kun: {failed_snd['Days_From_Last_In']} (Min: {MIN_DAYS_ON_SHELF}) | Qoldiq: {failed_snd['Hisoblangan_Qoldiq']}",
                **shop_info
            })



    # =========================================================
    # 5. SAQLASH
    # =========================================================
    for shop in MY_SHOPS:
        row_summary[f"{shop}_Yakuniy_Qoldiq"] = row_summary[f"{shop}_Hozirgi_Qoldiq"] + row_summary[f"{shop}_Transfer_Harakat"]
    summary_list.append(row_summary)

    def sort_c(df):
        if df.empty: return df
        # 'Status_Tarix' ni boshiga qo'shdik
        base = ['Подкатегория', 'Status_Tarix', 'Артикул', 'Цвет', 'Qayerdan', 'Qayerga', 'Soni', 'Sabab', 'Donor_Foiz']
        return df[[c for c in base if c in df.columns] + [c for c in df.columns if c not in base]]
    safe_podkat = TARGET_PODKAT.replace('/', '_')

    # Sanani fayl nomi uchun to'g'irlash
    sana_str = TODAY.strftime("%Y-%m-%d__%H-%M")
    file_out = f"PEREMESHINYA_{safe_podkat}_{sana_str}.xlsx"

    # 2-O'ZGARISH: df_stats ga Podkategoriya ustunini qo'shamiz
    df_stats['Подкатегория'] = TARGET_PODKAT

    # Transferlar jadvaliga ham qo'shamiz (agar transfer bo'lsa)
    for item in transfer_list:
        item['Подкатегория'] = TARGET_PODKAT

    with pd.ExcelWriter(file_out, engine='openpyxl') as writer:
        # Transferlar varag'i
        if transfer_list:
            sort_c(pd.DataFrame(transfer_list)).to_excel(writer, sheet_name='Transferlar', index=False)
        else:
            pd.DataFrame({'Info':['Transfer yo\'q']}).to_excel(writer, sheet_name='Transferlar', index=False)

        # Batafsil Hisob (debug qo‘shildi)
        if skipped_list:
            df_skipped = pd.DataFrame(skipped_list)
            df_skipped['Подкатегория'] = TARGET_PODKAT
            sort_c(df_skipped).to_excel(writer, sheet_name='Batafsil_Hisob', index=False)
        else:
            # Agar hamma transfer qilinsa, transfer_list bilan birga chiqarish mumkin
            df_stats_copy = df_stats.copy()
            df_stats_copy['Debug'] = 'Transfer qilindi'
            df_stats_copy['Подкатегория'] = TARGET_PODKAT
            sort_c(df_stats_copy).to_excel(writer, sheet_name='Batafsil_Hisob', index=False)

        # Statistika varag'i
        pd.DataFrame(summary_list).to_excel(writer, sheet_name='Statistika', index=False)


    print(f"\n✅ TAYYOR! '{file_out}'")
    print(f"✅ 'Подкатегория' ustuni qo'shildi.")
    stats = {
    'gives': {},
    'receives': {}
    }

    for item in transfer_list:
        snd = item['Qayerdan']
        rcv = item['Qayerga']
        qty = item['Soni']

        stats['gives'][snd] = stats['gives'].get(snd, 0) + qty
        stats['receives'][rcv] = stats['receives'].get(rcv, 0) + qty

    return file_out, stats


if __name__ == "__main__":
    run_transfer_analysis(
        FILE_NAME="Billz_Tarix_Batafsil.csv",
        
    )

