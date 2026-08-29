"""TradingView OCR -> Google Sheets, corrected mapping/version."""

import io
import json
import os
import re
from datetime import datetime

import cv2
import easyocr
import gspread
import numpy as np
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_config():
    return {
        "CREDENTIALS_JSON": os.getenv("GOOGLE_CREDENTIALS_JSON"),
        "PARENT_FOLDER_ID": os.getenv("PARENT_FOLDER_ID"),
        "SPREADSHEET_NAME": os.getenv("SPREADSHEET_NAME", "Market Analysis"),
    }


def get_authenticated_services(config):
    credentials_json = config["CREDENTIALS_JSON"]
    if credentials_json:
        credentials = Credentials.from_service_account_info(
            json.loads(credentials_json), scopes=SCOPES
        )
    else:
        credentials = Credentials.from_service_account_file(
            "credentials.json", scopes=SCOPES
        )

    return (
        build("drive", "v3", credentials=credentials),
        gspread.authorize(credentials),
    )


def get_latest_date_folder(drive_service, parent_id):
    query = (
        f"'{parent_id}' in parents and "
        "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )
    folders = (
        drive_service.files()
        .list(q=query, fields="files(id,name)")
        .execute()
        .get("files", [])
    )
    folders = [folder for folder in folders if re.fullmatch(r"\d{8}", folder["name"])]
    if not folders:
        return None, None
    latest = max(folders, key=lambda folder: folder["name"])
    return latest["name"], latest["id"]


def download_image_as_bytes(drive_service, file_id):
    request = drive_service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def number(value):
    if value is None:
        return None
    cleaned = re.sub(r"[^0-9.+-]", "", value.replace(",", ""))
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def ocr_text(reader, image):
    return " ".join(reader.readtext(image, detail=0)).replace("−", "-")


def extract_colored_number(hsv_image, reader, lower_hsv, upper_hsv):
    mask = cv2.inRange(hsv_image, lower_hsv, upper_hsv)
    mask = cv2.resize(mask, None, fx=3, fy=3, interpolation=cv2.INTER_CUBIC)
    text = ocr_text(reader, mask)
    matches = re.findall(r"\d[\d,.]*", text)
    return number(matches[-1]) if matches else None


def parse_chart_data(reader, image_bytes):
    """Read OHLC/change from row 1 and SMA values from row 2 only."""
    image = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("เปิดไฟล์ภาพไม่ได้")

    height, width = image.shape[:2]

    # TradingView screenshot samples: OHLC is around 5-10% of image height.
    ohlc_roi = image[
        int(height * 0.045) : int(height * 0.105),
        0 : int(width * 0.78),
    ]
    # SMA legend is the next row. Keeping it separate prevents cyan OPEN
    # from being incorrectly selected as SMA40.
    sma_roi = image[
        int(height * 0.085) : int(height * 0.145),
        0 : int(width * 0.72),
    ]

    ohlc_text = ocr_text(reader, ohlc_roi)
    sma_text = ocr_text(reader, sma_roi)
    print(f"OCR OHLC: {ohlc_text}")
    print(f"OCR SMA : {sma_text}")

    data = {
        "OPEN": None,
        "HIGH": None,
        "LOW": None,
        "CLOSE": None,
        "CHANGE": None,
        "CHANGE_PCT": None,
        "SMA20": None,
        "SMA40": None,
    }

    ohlc_pattern = re.compile(
        r"\bO\s*([\d,.]+).*?"
        r"\bH\s*([\d,.]+).*?"
        r"\bL\s*([\d,.]+).*?"
        r"\bC\s*([\d,.]+)",
        re.IGNORECASE,
    )
    ohlc_match = ohlc_pattern.search(ohlc_text)
    if ohlc_match:
        data["OPEN"], data["HIGH"], data["LOW"], data["CLOSE"] = [
            number(value) for value in ohlc_match.groups()
        ]

    change_match = re.search(
        r"([+-]\s*[\d,.]+)\s*\(\s*([+-]?\s*[\d,.]+)\s*%\s*\)",
        ohlc_text,
    )
    if change_match:
        data["CHANGE"] = number(change_match.group(1))
        data["CHANGE_PCT"] = number(change_match.group(2))

    hsv_sma = cv2.cvtColor(sma_roi, cv2.COLOR_BGR2HSV)
    data["SMA20"] = extract_colored_number(
        hsv_sma,
        reader,
        np.array([5, 100, 100]),
        np.array([30, 255, 255]),
    )
    data["SMA40"] = extract_colored_number(
        hsv_sma,
        reader,
        np.array([80, 90, 90]),
        np.array([110, 255, 255]),
    )

    # Text fallback: in these charts the two values follow the MA Ribbon label.
    if data["SMA20"] is None or data["SMA40"] is None:
        tail = re.split(r"SMA\s*,?\s*200\)?", sma_text, maxsplit=1, flags=re.I)
        values = re.findall(r"\d[\d,.]*", tail[-1]) if len(tail) == 2 else []
        if len(values) >= 2:
            data["SMA20"] = data["SMA20"] or number(values[0])
            data["SMA40"] = data["SMA40"] or number(values[1])

    required = ["OPEN", "HIGH", "LOW", "CLOSE", "SMA20", "SMA40"]
    missing = [key for key in required if data[key] is None]
    if missing:
        raise ValueError(f"OCR อ่านค่าไม่ครบ: {', '.join(missing)}")

    return data


def canonical_date(value):
    text = str(value).strip()
    for pattern in ("%d/%m/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).strftime("%Y%m%d")
        except ValueError:
            pass
    return text


def find_date_row(worksheet, folder_date):
    target = canonical_date(folder_date)
    for row_number, value in enumerate(worksheet.col_values(1)[1:], start=2):
        if canonical_date(value) == target:
            return row_number
    return None


def main():
    print("เริ่มต้น Python OCR")
    config = get_config()
    if not config["PARENT_FOLDER_ID"]:
        raise RuntimeError("PARENT_FOLDER_ID is not set")

    drive_service, client = get_authenticated_services(config)
    spreadsheet = client.open(config["SPREADSHEET_NAME"])
    reader = easyocr.Reader(["en"], gpu=False)

    folder_date, folder_id = get_latest_date_folder(
        drive_service, config["PARENT_FOLDER_ID"]
    )
    if not folder_id:
        raise RuntimeError("ไม่พบโฟลเดอร์ชื่อ YYYYMMDD")

    date_object = datetime.strptime(folder_date, "%Y%m%d")
    display_date = date_object.strftime("%d/%m/%Y")
    print(f"พบโฟลเดอร์ล่าสุด: {folder_date}")

    query = f"'{folder_id}' in parents and mimeType contains 'image/' and trashed = false"
    files = (
        drive_service.files()
        .list(q=query, fields="files(id,name)")
        .execute()
        .get("files", [])
    )

    processed = 0
    for file in files:
        filename = file["name"]
        asset = filename.split("_")[0].upper()
        try:
            worksheet = spreadsheet.worksheet(asset)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(asset, rows=1000, cols=20)
            worksheet.append_row(
                [
                    "DATE", "FILENAME", "OPEN", "HIGH", "LOW", "CLOSE",
                    "CHANGE", "% CHANGE", "SMA20", "SMA40",
                ]
            )
            worksheet.format("1:1", {"textFormat": {"bold": True}})

        existing_row = find_date_row(worksheet, folder_date)
        if existing_row:
            print(f"ข้าม {asset}: มีวันที่ {display_date} ที่แถว {existing_row} แล้ว")
            continue

        print(f"กำลังอ่าน {asset}: {filename}")
        parsed = parse_chart_data(
            reader, download_image_as_bytes(drive_service, file["id"])
        )
        row = [
            display_date,
            filename,
            parsed["OPEN"],
            parsed["HIGH"],
            parsed["LOW"],
            parsed["CLOSE"],
            parsed["CHANGE"],
            parsed["CHANGE_PCT"],
            parsed["SMA20"],
            parsed["SMA40"],
        ]
        worksheet.append_row(row, value_input_option="USER_ENTERED")
        new_row = len(worksheet.col_values(1))
        worksheet.format(
            f"A{new_row}", {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}
        )
        processed += 1
        print(f"บันทึก {asset}: {row}")

    print(f"เสร็จสิ้น: บันทึก {processed} รายการ")


if __name__ == "__main__":
    main()
