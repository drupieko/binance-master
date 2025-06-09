# config/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Find the project root and .env
BASE_DIR = Path(__file__).resolve().parent.parent
dotenv_path = BASE_DIR / '.env'

# Load env vars
load_dotenv(dotenv_path=dotenv_path)

# Binance credentials
BINANCE_API_KEY    = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# Telegram credentials
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID')

# Ensure nothing is missing
required = {
    'BINANCE_API_KEY':    BINANCE_API_KEY,
    'BINANCE_API_SECRET': BINANCE_API_SECRET,
    'TELEGRAM_BOT_TOKEN': TELEGRAM_BOT_TOKEN,
    'TELEGRAM_CHAT_ID':   TELEGRAM_CHAT_ID,
}
missing = [k for k,v in required.items() if not v]
if missing:
    raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")
