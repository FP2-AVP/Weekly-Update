import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from tvDatafeed import TvDatafeed, Interval
import yfinance as yf
from datetime import datetime

# ==============================
# CONFIG
# ==============================
SPREADSHEET_NAME = "TradingView_PriceData"
LIST_SHEET = "Lists"

# ==============================
# GOOGLE SHEETS AUTH
# ==============================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    "credentials.json", scope
)
gc = gspread.authorize(creds)
spreadsheet = gc.open(SPREADSHEET_NAME)

# ==============================
# TVDATAFEED
# ==============================
tv = TvDatafeed()  # anonymous login

# ==============================
# HELPERS
# ==============================
def safe_date(df):
    df = df.copy()
    if isinstance(df.index, pd.DatetimeIndex):
        df["date"] = df.index.strftime("%Y-%m-%d")
        df.reset_index(drop=True, inplace=True)
    return df

def upload_to_sheet(sheet_name, df):
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(df) + 5),
            cols=str(len(df.columns) + 5)
        )

    worksheet.update(
        [df.columns.tolist()] + df.astype(str).values.tolist()
    )

# ==============================
# FETCH FROM YAHOO
# ==============================
def fetch_yahoo(symbol):
    df = yf.download(symbol, auto_adjust=False, progress=False)

    if df.empty or "Adj Close" not in df.columns:
        return None

    df = df[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
    df = safe_date(df)
    df["source"] = "yahoo"
    return df

# ==============================
# FETCH FROM TRADINGVIEW
# ==============================
def fetch_tradingview(symbol):
    try:
        exchange, ticker = symbol.split(":")
    except ValueError:
        return None

    df = tv.get_hist(
        symbol=ticker,
        exchange=exchange,
        interval=Interval.in_daily,
        n_bars=5000
    )

    if df is None or df.empty:
        return None

    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })

    df = df[["Open", "High", "Low", "Close", "Volume"]]
    df["Adj Close"] = df["Close"]  # TradingView ไม่มี adj
    df = safe_date(df)
    df["source"] = "tradingview"
    return df

# ==============================
# MAIN
# ==============================
def main():
    list_ws = spreadsheet.worksheet(LIST_SHEET)
    rows = list_ws.get_all_records()

    for row in rows:
        yahoo_symbol = row.get("Yahoo Symbol")
        tv_symbol = row.get("TradingView Symbol")
        sheet_name = row.get("Sheet Name")

        print(f"📥 {sheet_name}")

        df = None

        # 1) Try Yahoo
        if yahoo_symbol:
            df = fetch_yahoo(yahoo_symbol)

        # 2) Fallback TradingView
        if df is None and tv_symbol:
            df = fetch_tradingview(tv_symbol)

        if df is None:
            print(f"⚠️ {sheet_name}: ไม่มีข้อมูล")
            continue

        upload_to_sheet(sheet_name, df)
        print(f"✅ {sheet_name}: {len(df)} rows")

    print("✨ เสร็จสิ้น")

# ==============================
if __name__ == "__main__":
    main()
