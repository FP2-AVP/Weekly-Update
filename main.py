import os
import json
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tvDatafeed import TvDatafeed, Interval

# ==================================================
# CONFIG
# ==================================================
SPREADSHEET_ID = "PUT_YOUR_SPREADSHEET_ID_HERE"
LISTS_SHEET = "Lists"
START_DATE = "2015-01-01"

FINAL_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume"
]

# ==================================================
# GOOGLE SHEETS AUTH (GitHub Actions)
# ==================================================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gc = gspread.authorize(creds)

spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# ==================================================
# DATA SOURCES
# ==================================================
tv = TvDatafeed()


def fetch_yahoo(symbol: str) -> pd.DataFrame:
    df = yf.download(symbol, start=START_DATE, progress=False)

    if df.empty:
        raise ValueError("Yahoo: no data")

    df = df.reset_index()

    df["date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df = df[[
        "date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume"
    ]]

    df.columns = FINAL_COLUMNS
    return df


def fetch_tradingview(symbol: str) -> pd.DataFrame:
    df = tv.get_hist(
        symbol=symbol,
        exchange=None,
        interval=Interval.in_daily,
        n_bars=5000
    )

    if df is None or df.empty:
        raise ValueError("TradingView: no data")

    df = df.reset_index()

    df["date"] = pd.to_datetime(
        df["datetime"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df = df[[
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]]

    df["adj_close"] = df["close"]

    df = df[FINAL_COLUMNS]
    return df


# ==================================================
# GOOGLE SHEET WRITE
# ==================================================
def write_to_sheet(sheet_name: str, df: pd.DataFrame):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(df) + 10),
            cols=str(len(df.columns) + 5)
        )

    ws.update(
        [df.columns.tolist()] +
        df.astype(str).values.tolist()
    )


# ==================================================
# MAIN LOGIC
# ==================================================
def main():
    print("🚀 เริ่มดึงข้อมูล (Daily)...")

    lists_ws = spreadsheet.worksheet(LISTS_SHEET)
    rows = lists_ws.get_all_records()

    for row in rows:
        yahoo_symbol = str(row.get("Yahoo Symbol", "")).strip()
        tv_symbol = str(row.get("TradingView Symbol", "")).strip()
        sheet_name = str(row.get("Sheet Name", "")).strip()

        if not sheet_name:
            continue

        try:
            if yahoo_symbol:
                print(f"📥 {sheet_name} ← Yahoo ({yahoo_symbol})")
                df = fetch_yahoo(yahoo_symbol)

            elif tv_symbol:
                print(f"📥 {sheet_name} ← TradingView ({tv_symbol})")
                df = fetch_tradingview(tv_symbol)

            else:
                print(f"⚠️ {sheet_name}: ไม่มี Symbol")
                continue

            write_to_sheet(sheet_name, df)
            print(f"✅ {sheet_name}: {len(df)} rows")

        except Exception as e:
            print(f"❌ {sheet_name} error: {e}")

    print("✨ เสร็จสิ้น")


if __name__ == "__main__":
    main()
