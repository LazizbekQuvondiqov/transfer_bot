import logging
import os
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command 

from transfer_analysis import run_transfer_analysis, update_db_by_category
from config import *
from logic import (
    update_catalog_only, 
    run_markdown_analysis, 
    run_advanced_sales_analysis, 
    get_main_categories, 
    get_subcategories_by_cat,
    is_cache_valid,
    init_cache_table
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.ERROR)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- MENYU ---
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔄 Katalogni Yangilash")],
        [KeyboardButton(text="📊 Analiz Boshlash"), KeyboardButton(text="📉 Skidka Analiz")],
        [KeyboardButton(text="💸 To'liq Analiz")]
    ],
    resize_keyboard=True
)

# --- START ---
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    await message.answer("👋 <b>Xush kelibsiz!</b>\nTanlang:", reply_markup=main_kb, parse_mode="HTML")

# --- 1. KATALOGNI YANGILASH ---
@dp.message(F.text == "🔄 Katalogni Yangilash")
async def update_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    status_msg = await message.answer("⏳ <b>Katalog yangilanmoqda...</b>", parse_mode="HTML")

    loop = asyncio.get_event_loop()
    success, msg = await loop.run_in_executor(None, update_catalog_only)

    await status_msg.delete()
    await message.answer(f"✅ {msg}" if success else f"❌ {msg}", parse_mode="HTML")

# --- 2. TO'LIQ ANALIZ (Mavsumiy + Lost Sales) ---
@dp.message(F.text == "💸 To'liq Analiz")
async def full_analysis_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    status_msg = await message.answer("💸 <b>To'liq Analiz...</b>\n⏳ Hisoblanmoqda...", parse_mode="HTML")

    loop = asyncio.get_event_loop()
    file_path, info_text = await loop.run_in_executor(None, run_advanced_sales_analysis)

    await status_msg.delete()
    if not file_path:
        await message.answer(info_text)
        return
    try:
        await message.answer_document(types.FSInputFile(file_path), caption=info_text, parse_mode="HTML")
        os.remove(file_path)
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")

# =========================================================================
#  A) TRANSFER ANALIZ (Lider/Donor)
# =========================================================================
@dp.message(F.text == "📊 Analiz Boshlash")
async def category_menu(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    
    # 1. Avval bor kategoriyalarni tekshiramiz
    cats = get_main_categories()

    # 2. Agar baza bo'sh bo'lsa -> AVTOMATIK YANGILASHNI BOSHLAYMIZ
    if not cats:
        wait_msg = await message.answer("🔄 <b>Baza bo'sh ekan. Katalog avtomatik yangilanmoqda...</b>\nIltimos, kuting (1-2 daqiqa).", parse_mode="HTML")
        
        loop = asyncio.get_event_loop()
        # Katalogni yangilash funksiyasini chaqiramiz
        success, msg = await loop.run_in_executor(None, update_catalog_only)
        
        if success:
            # Yangilangandan keyin qaytadan o'qiymiz
            cats = get_main_categories()
            await wait_msg.delete()
            await message.answer(f"✅ {msg}", parse_mode="HTML")
        else:
            await wait_msg.edit_text(f"❌ Xatolik: {msg}")
            return

    # 3. Agar shunda ham bo'sh bo'lsa (demak saytda tovar yo'q)
    if not cats:
        await message.answer("❌ Tizimda kategoriyalar topilmadi.")
        return

    # 4. Menyuni chizamiz
    kb_list = []
    for i in range(0, len(cats), 2):
        row = [InlineKeyboardButton(text=f"📁 {cats[i]}", callback_data=f"cat:{cats[i]}")]
        if i + 1 < len(cats):
            row.append(InlineKeyboardButton(text=f"📁 {cats[i+1]}", callback_data=f"cat:{cats[i+1]}"))
        kb_list.append(row)

    await message.answer("📂 <b>Transfer uchun kategoriya tanlang:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), parse_mode="HTML")

@dp.callback_query(F.data.startswith("cat:"))
async def sub_menu(callback: CallbackQuery):
    cat_name = callback.data.split(":", 1)[1]
    subcats = get_subcategories_by_cat(cat_name)

    if not subcats:
        await callback.message.edit_text("❌ Bu kategoriyada podkategoriya yo'q.")
        return

    kb_list = []
    for i in range(0, len(subcats), 2):
        row = [InlineKeyboardButton(text=f"📦 {subcats[i]}", callback_data=f"sub:{subcats[i]}")]
        if i + 1 < len(subcats):
            row.append(InlineKeyboardButton(text=f"📦 {subcats[i+1]}", callback_data=f"sub:{subcats[i+1]}"))
        kb_list.append(row)
    kb_list.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="back_to_cats")])

    await callback.message.edit_text(
        f"📂 <b>{cat_name}</b>\nPodkategoriyani tanlang:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), 
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back_to_cats")
async def back_to_cats(callback: CallbackQuery):
    await category_menu(callback.message)

# --- ENG MUHIM QISM: ANALIZ HANDLERI ---
@dp.callback_query(F.data.startswith("sub:"))
async def run_transfer_job(callback: CallbackQuery):
    sub_cat = callback.data.split(":", 1)[1]
    
    # 1. BAZANI YANGILASH
    await callback.message.edit_text(
        f"🔄 <b>{sub_cat}</b>\n1. API dan ma'lumot olinmoqda...\nIltimos kuting.", 
        parse_mode="HTML"
    )

    loop = asyncio.get_event_loop()
    
    # Yangi funksiyani chaqiramiz
    success, msg = await loop.run_in_executor(None, update_db_by_category, sub_cat)
    
    if not success:
        await callback.message.edit_text(msg)
        return

    # 2. ANALIZ QILISH
    await callback.message.edit_text(
        f"{msg}\n\n2. ⏳ Analiz qilinmoqda va fayllar tayyorlanmoqda...", 
        parse_mode="HTML"
    )

    csv_name = "Billz_Tarix_Batafsil.csv"
    
    # Analiz funksiyasini chaqiramiz
    excel_path, stats = await loop.run_in_executor(
        None, 
        run_transfer_analysis, 
        csv_name, 
        sub_cat
    )

    # 3. CSV YUBORISH
    if os.path.exists(csv_name):
        try:
            csv_file = types.FSInputFile(os.path.abspath(csv_name))
            await callback.message.answer_document(
                csv_file,
                caption=f"📂 <b>Raw Data (CSV)</b>\nKategoriya: {sub_cat}",
                parse_mode="HTML"
            )
        except Exception as e:
            await callback.message.answer(f"⚠️ CSV yuborishda xato: {e}")

# 4. EXCEL YUBORISH
    if excel_path and os.path.exists(excel_path):
        # Statistika matnini tayyorlash
        text = f"📊 <b>TRANSFER ANALIZ: {sub_cat}</b>\n\n"
        
        # ✅ BERUVCHILARNI SORTLASH (Ko'pdan kamga)
        gives = stats.get('gives', {})
        if gives:
            text += f"<b>Beruvchilar:</b>\n"
            # Sort: kamayib boruvchi tartibda
            sorted_gives = sorted(gives.items(), key=lambda x: x[1], reverse=True)
            for shop, qty in sorted_gives:
                if qty > 0:
                    text += f" • {shop}: {int(qty)} ta\n"
        
        # ✅ OLUVCHILARNI SORTLASH (Ko'pdan kamga)
        receives = stats.get('receives', {})
        if receives:
            text += f"\n<b>Oluvchilar:</b>\n"
            # Sort: kamayib boruvchi tartibda
            sorted_receives = sorted(receives.items(), key=lambda x: x[1], reverse=True)
            for shop, qty in sorted_receives:
                if qty > 0:
                    text += f" • {shop}: {int(qty)} ta\n"

        if len(text) > 1000: text = text[:1000] + "..."

        try:
            excel_file = types.FSInputFile(os.path.abspath(excel_path))
            await callback.message.delete()
            
            await callback.message.answer_document(
                excel_file, 
                caption=text, 
                parse_mode="HTML"
            )
            
            await asyncio.sleep(2)
            os.remove(os.path.abspath(excel_path))

        except Exception as e:
            await callback.message.answer(f"❌ Excel yuborishda xato: {e}")
    else:
        await callback.message.delete()
        await callback.message.answer("⚠️ Transfer uchun ma'lumot topilmadi yoki hamma shartlar bajarilgan.")

    # Menyuga qaytish
    await category_menu(callback.message)

# =========================================================================
#  B) SKIDKA ANALIZ (Markdown)
# =========================================================================
@dp.message(F.text == "📉 Skidka Analiz")
async def markdown_menu(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    cats = get_main_categories()
    if not cats:
        await message.answer("❌ Baza bo'sh.")
        return

    kb_list = []
    for i in range(0, len(cats), 2):
        row = [InlineKeyboardButton(text=f"📉 {cats[i]}", callback_data=f"mcat:{cats[i]}")]
        if i + 1 < len(cats):
            row.append(InlineKeyboardButton(text=f"📉 {cats[i+1]}", callback_data=f"mcat:{cats[i+1]}"))
        kb_list.append(row)

    await message.answer("📉 <b>Skidka uchun kategoriya tanlang:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), parse_mode="HTML")

@dp.callback_query(F.data.startswith("mcat:"))
async def markdown_sub_menu(callback: CallbackQuery):
    cat_name = callback.data.split(":", 1)[1]
    subcats = get_subcategories_by_cat(cat_name)

    kb_list = []
    for i in range(0, len(subcats), 2):
        row = [InlineKeyboardButton(text=f"🔻 {subcats[i]}", callback_data=f"msub:{subcats[i]}")]
        if i + 1 < len(subcats):
            row.append(InlineKeyboardButton(text=f"🔻 {subcats[i+1]}", callback_data=f"msub:{subcats[i+1]}"))
        kb_list.append(row)
    kb_list.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="back_to_mcats")])

    await callback.message.edit_text(f"📉 <b>{cat_name}</b> (Skidka)\nPodkategoriyani tanlang:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list), parse_mode="HTML")

@dp.callback_query(F.data == "back_to_mcats")
async def back_to_markdown_cats(callback: CallbackQuery):
    await markdown_menu(callback.message)

@dp.callback_query(F.data.startswith("msub:"))
async def run_markdown_job(callback: CallbackQuery):
    sub_cat = callback.data.split(":", 1)[1]
    status_msg = await callback.message.edit_text(f"📉 <b>{sub_cat}</b> (Skidka)...\nHisoblanmoqda...", parse_mode="HTML")

    loop = asyncio.get_event_loop()
    file_path, stats_msg = await loop.run_in_executor(None, run_markdown_analysis, sub_cat, None)

    await status_msg.delete()
    if not file_path:
        await callback.message.answer(stats_msg)
        return

    try:
        caption = f"📉 <b>SKIDKA: {sub_cat}</b>\n\n{stats_msg}"
        await callback.message.answer_document(types.FSInputFile(file_path), caption=caption, parse_mode="HTML")
        os.remove(file_path)
    except Exception as e:
        await callback.message.answer(f"❌ Fayl yuborishda xato: {e}")

    await markdown_menu(callback.message)

# --- MAIN FUNKSIYASI ---
async def main():
    # ✅ 1. KESH JADVALINI YARATISH (Birinchi marta ishga tushganda)
    print("🔧 Kesh jadvali tekshirilmoqda...")
    init_cache_table()
    
    # ✅ 2. WEBHOOK NI O'CHIRISH (Polling rejimida ishlash uchun)
    await bot.delete_webhook(drop_pending_updates=True)
    
    # ✅ 3. BOT ISHGA TUSHMOQDA
    print("🤖 Bot ishga tushdi (Polling rejimi)...")
    print(f"👤 Admin ID: {ADMIN_ID}")
    
    # ✅ 4. POLLING BOSHLANMOQDA
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())