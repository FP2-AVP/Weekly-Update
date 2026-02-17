import json, os, time
import pandas as pd
import gspread
import yfinance as yf
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime

# ===============================
# 🔐 Google Service Account
# ===============================
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
gc = gspread.service_account_from_dict(creds_dict)

# ===============================
# 📄 Google Sheet
# ===============================
SPREADSHEET_ID = "1t2F5tH9t8G41qWhXEoSQFVwviIpQJ9USmRKTvtKxh8Q"
sh = gc.open_by_key(SPREADSHEET_ID)
list_sheet = sh.worksheet("Lists")

symbols_list = list_sheet.get("C3:C32")
names_list   = list_sheet.get("D3:D32")

FINAL_COLS = [
    "Datetime","Symbol","Open","High","Low",
    "Close","Volume","Date","Adj Close","Source"
]

# ===============================
# 📈 TradingView
# ===============================
tv = TvDatafeed()

# ===============================
# 📊 Yahoo helper
# ===============================
def get_from_yahoo(symbol, start_date):
    try:
        yf_symbol = symbol.replace(":", "-")
        df = yf.download(
            yf_symbol,
            start=start_date.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=False
        )

        if df.empty or "Adj Close" not in df.columns:
            return None

        df = df.reset_index()
        df["Symbol"] = symbol
        df["Datetime"] = df["Date"]
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        df["Source"] = "Yahoo"

        return df[[
            "Datetime","Symbol",
            "Open","High","Low",
            "Close","Volume","Date","Adj Close","Source"
        ]]
    except:
        return None


# ===============================
# 📉 TradingView helper
# ===============================
def get_from_tradingview(symbol):
    df = tv.get_hist(
        symbol=symbol,
        exchange="",
        interval=Interval.in_daily,
        n_bars=2000
    )

    if df is None or df.empty:
        return None

    df = df.reset_index()
    df["Symbol"] = symbol
    df["Date"] = df["datetime"].dt.strftime("%Y-%m-%d")
    df["Adj Close"] = df["close"]
    df["Source"] = "TradingView"

    return df[[
        "datetime","Symbol",
        "open","high","low",
        "close","volume","Date","Adj Close","Source"
    ]].rename(columns={
        "datetime":"Datetime",
        "open":"Open",
        "high":"High",
        "low":"Low",
        "close":"Close",
        "volume":"Volume"
    })


print("🚀 เริ่มดึงข้อมูล (Daily)...")

for s_row, n_row in zip(symbols_list, names_list):
    if not s_row or not n_row:
        continue

    symbol = s_row[0].strip()
    sheet_name = n_row[0].strip()

    try:
        # ===============================
        # 🧾 Worksheet
        # ===============================
        try:
            worksheet = sh.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(sheet_name, rows="2000", cols="30")
            worksheet.update("A1:J1", [FINAL_COLS])

        existing = worksheet.get_all_values()
        last_date = (
            pd.to_datetime(
                pd.DataFrame(existing[1:], columns=existing[0])["Datetime"]
            ).max()
            if len(existing) > 1 else datetime(2000,1,1)
        )

        # ===============================
        # 🔁 Yahoo → TradingView fallback
        # ===============================
        df = get_from_yahoo(symbol, last_date)

        if df is None:
            df = get_from_tradingview(symbol)

        if df is None or df.empty:
            print(f"⚠️ {symbol}: ไม่มีข้อมูล")
            continue

        df = df[df["Datetime"] > last_date]

        if df.empty:
            continue

        worksheet.append_rows(
            df.values.tolist(),
            value_input_option="USER_ENTERED"
        )

        print(f"✅ {symbol}: เพิ่ม {len(df)} แถว ({df.iloc[0]['Source']})")

    except Exception as e:
        print(f"❌ {symbol} error: {e}")

    time.sleep(1)

print("✨ เสร็จสิ้น")
