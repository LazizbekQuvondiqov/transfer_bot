import os
from pathlib import Path
from dotenv import load_dotenv

# -----------------------------
# Load .env file safely
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# -----------------------------
# Helper function to ensure required env variables exist
# -----------------------------
def must(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable '{name}' not found in .env")
    return value

# -----------------------------
# Bot Configuration
# -----------------------------
BOT_TOKEN = must("BOT_TOKEN")
ADMIN_ID = int(must("ADMIN_ID"))

# -----------------------------
# Webhook Configuration
# -----------------------------
# WEBHOOK_HOST = must("WEBHOOK_HOST")
# WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
# WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# WEBAPP_HOST = "0.0.0.0"
# WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", 8000))  # default 8000

# -----------------------------
# API Configuration
# -----------------------------
SECRET_KEY = must("SECRET_KEY")
BROWSER_TOKEN = must("BROWSER_TOKEN")
PLATFORM_ID = must("PLATFORM_ID")
COOKIE_VALUE = must("COOKIE_VALUE")

# -----------------------------
# API Endpoints
# -----------------------------
ADMIN_BASE = "https://api-admin.billz.io"
HISTORY_BASE_URL = "https://buttonshop.billz.io"

# -----------------------------
# Database / Files
# -----------------------------
DB_FILE = BASE_DIR / "transfer.db"
CSV_FILE = BASE_DIR / "Billz_Tarix_Batafsil2.csv"

# -----------------------------
# Shops Configuration
# -----------------------------
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
    "c91a913b-c295-4775-a7a8-4a0ce2578fa0":"СКЛАД БРАКА",
    "559bfd04-be37-4a9f-ab5b-3af44ccf524d":"Aziz"
}
