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
    "Close","Volume","Date","Adj Close"
]

# ===============================
# 📈 TradingView
# ===============================
tv = TvDatafeed()

# ===============================
# 📊 Yahoo Adj Close helper
# ===============================
def get_yahoo_adj_close(symbol, start_date):
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
        return df["Adj Close"]
    except Exception:
        return None


print("🚀 เริ่มดึงข้อมูล (Daily)...")

for s_row, n_row in zip(symbols_list, names_list):
    if not s_row or not n_row:
        continue

    symbol = s_row[0].strip()
    target_sheet_name = n_row[0].strip()

    try:
        # ===============================
        # 🧾 Worksheet
        # ===============================
        try:
            worksheet = sh.worksheet(target_sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(
                title=target_sheet_name, rows="2000", cols="25"
            )
            worksheet.update("A1:I1", [FINAL_COLS])

        existing = worksheet.get_all_values()
        last_date = (
            pd.to_datetime(
                pd.DataFrame(existing[1:], columns=existing[0])["Datetime"]
            ).max()
            if len(existing) > 1 else None
        )

        # ===============================
        # 📥 TradingView data
        # ===============================
        df_new = tv.get_hist(
            symbol=symbol,
            exchange="",
            interval=Interval.in_daily,
            n_bars=2000
        )

        if df_new is None or df_new.empty:
            continue

        df_new = df_new.reset_index()

        if last_date is not None:
            df_new = df_new[df_new["datetime"] > last_date]

        if df_new.empty:
            continue

        # ===============================
        # 📊 Yahoo Adj Close
        # ===============================
        start_date = df_new["datetime"].min()
        adj_series = get_yahoo_adj_close(symbol, start_date)

        # ===============================
        # 📤 Prepare rows
        # ===============================
        data = []

        for _, r in df_new.iterrows():
            date_key = r["datetime"].strftime("%Y-%m-%d")

            adj_close = (
                float(adj_series.loc[date_key])
                if adj_series is not None and date_key in adj_series.index
                else r["close"]  # fallback
            )

            data.append([
                r["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
                date_key,
                adj_close
            ])

        worksheet.append_rows(data, value_input_option="USER_ENTERED")
        print(f"✅ {symbol}: เพิ่ม {len(data)} แถว")

    except Exception as e:
        print(f"❌ {symbol} error: {e}")

    time.sleep(1)

print("✨ เสร็จสิ้น")
