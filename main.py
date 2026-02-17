import pandas as pd
import yfinance as yf
from tvDatafeed import TvDatafeed, Interval
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ===============================
# CONFIG
# ===============================
SPREADSHEET_NAME = "YOUR_SPREADSHEET_NAME"
LISTS_SHEET = "Lists"

START_DATE = "2015-01-01"
INTERVAL = Interval.in_daily

# ===============================
# GOOGLE SHEETS AUTH
# ===============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "service_account.json", scope
)
gc = gspread.authorize(creds)
spreadsheet = gc.open(SPREADSHEET_NAME)

# ===============================
# TRADINGVIEW INIT
# ===============================
tv = TvDatafeed()

# ===============================
# HELPERS
# ===============================
def fetch_yahoo(symbol: str) -> pd.DataFrame:
    df = yf.download(symbol, start=START_DATE, progress=False)

    if df.empty:
        raise ValueError("Yahoo: No data")

    df = df.reset_index()

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df = df[[
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume"
    ]]

    df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume"
    ]

    return df


def fetch_tradingview(symbol: str) -> pd.DataFrame:
    df = tv.get_hist(
        symbol=symbol,
        exchange=None,
        interval=INTERVAL,
        n_bars=5000
    )

    if df is None or df.empty:
        raise ValueError("TradingView: No data")

    df = df.reset_index()

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")

    df = df[[
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]]

    df["adj_close"] = df["close"]

    return df


def write_to_sheet(sheet_name: str, df: pd.DataFrame):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(df) + 5),
            cols=str(len(df.columns) + 5)
        )

    ws.update(
        [df.columns.tolist()] + df.astype(str).values.tolist()
    )


# ===============================
# MAIN
# ===============================
def main():
    print("🚀 เริ่มดึงข้อมูล (Daily)...")

    lists_ws = spreadsheet.worksheet(LISTS_SHEET)
    rows = lists_ws.get_all_records()

    for row in rows:
        yahoo_symbol = row.get("Yahoo Symbol", "").strip()
        tv_symbol = row.get("TradingView Symbol", "").strip()
        sheet_name = row.get("Sheet Name", "").strip()

        if not sheet_name:
            continue

        try:
            if yahoo_symbol:
                print(f"📥 {sheet_name}: Yahoo ({yahoo_symbol})")
                df = fetch_yahoo(yahoo_symbol)

            elif tv_symbol:
                print(f"📥 {sheet_name}: TradingView ({tv_symbol})")
                df = fetch_tradingview(tv_symbol)

            else:
                print(f"⚠️ {sheet_name}: ไม่มี Symbol")
                continue

            write_to_sheet(sheet_name, df)
            print(f"✅ {sheet_name}: สำเร็จ ({len(df)} rows)")

        except Exception as e:
            print(f"❌ {sheet_name} error: {e}")

    print("✨ เสร็จสิ้น")


if __name__ == "__main__":
    main()
