"""TradingView OCR -> Google Sheets, corrected mapping/version."""

import io
import json
import os
import re
from datetime import date, datetime

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
    # In a numeric position EasyOCR may confuse zero with the letter O.
    value = value.replace("O", "0").replace("o", "0")
    cleaned = re.sub(r"[^0-9.+-]", "", value.replace(",", ""))
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def normalize_ocr_text(text):
    """Normalize common EasyOCR substitutions."""
    return (
        str(text)
        .replace("−", "-")
        .replace("–", "-")
        .replace("—", "-")
        .replace("~", "-")
        .replace("CMA", "SMA")
        .replace("CmA", "SMA")
        .replace("cMA", "SMA")
    )


def ocr_text(reader, image):
    return normalize_ocr_text(" ".join(reader.readtext(image, detail=0)))


def read_positioned_lines(reader, image, scale=1.0):
    """OCR a region and group detected boxes into visual text lines."""
    scan = image
    if scale != 1.0:
        scan = cv2.resize(
            image, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC
        )

    raw = reader.readtext(scan, detail=1, paragraph=False)
    items = []
    for box, text, confidence in raw:
        xs = [point[0] / scale for point in box]
        ys = [point[1] / scale for point in box]
        items.append(
            {
                "text": normalize_ocr_text(text),
                "confidence": float(confidence),
                "x1": min(xs),
                "x2": max(xs),
                "y1": min(ys),
                "y2": max(ys),
                "cy": (min(ys) + max(ys)) / 2,
                "height": max(ys) - min(ys),
            }
        )

    if not items:
        return []

    median_height = float(np.median([item["height"] for item in items]))
    y_tolerance = max(6.0, median_height * 0.75)
    lines = []

    for item in sorted(items, key=lambda value: (value["cy"], value["x1"])):
        best_line = None
        best_distance = None
        for line in lines:
            distance = abs(item["cy"] - line["cy"])
            if distance <= y_tolerance and (
                best_distance is None or distance < best_distance
            ):
                best_line = line
                best_distance = distance

        if best_line is None:
            lines.append({"items": [item], "cy": item["cy"]})
        else:
            best_line["items"].append(item)
            best_line["cy"] = sum(
                value["cy"] for value in best_line["items"]
            ) / len(best_line["items"])

    output = []
    for line in sorted(lines, key=lambda value: value["cy"]):
        line_items = sorted(line["items"], key=lambda value: value["x1"])
        output.append(
            {
                "text": normalize_ocr_text(
                    " ".join(value["text"] for value in line_items)
                ),
                "x1": min(value["x1"] for value in line_items),
                "x2": max(value["x2"] for value in line_items),
                "y1": min(value["y1"] for value in line_items),
                "y2": max(value["y2"] for value in line_items),
                "confidence": sum(
                    value["confidence"] for value in line_items
                ) / len(line_items),
            }
        )
    return output


def extract_colored_number(hsv_image, reader, lower_hsv, upper_hsv):
    """Read the last number in one color-masked legend row."""
    mask = cv2.inRange(hsv_image, lower_hsv, upper_hsv)
    mask = cv2.resize(mask, None, fx=3, fy=3, interpolation=cv2.INTER_CUBIC)
    text = ocr_text(reader, mask)
    matches = re.findall(r"(?:[0O]?[.,]\d+|\d[\d,.]*)", text)
    return number(matches[-1]) if matches else None


def empty_chart_data():
    return {
        "OPEN": None,
        "HIGH": None,
        "LOW": None,
        "CLOSE": None,
        "CHANGE": None,
        "CHANGE_PCT": None,
        "SMA20": None,
        "SMA40": None,
    }


def parse_ohlc_candidate(text):
    """Parse one candidate line and return its values and match score."""
    text = normalize_ocr_text(text)
    data = empty_chart_data()
    numeric_token = r"((?:[0O]?[.,]\d+|\d[\d,.]*))"
    patterns = {
        "OPEN": rf"(?<![A-Z0-9])[O0]\s*[:=]?\s*{numeric_token}",
        "HIGH": rf"(?<![A-Z0-9])H\s*[:=]?\s*{numeric_token}",
        "LOW": rf"(?<![A-Z0-9])L\s*[:=]?\s*{numeric_token}",
        "CLOSE": rf"(?<![A-Z0-9])C\s*[:=]?\s*{numeric_token}",
    }

    label_matches = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data[key] = number(match.group(1))
            label_matches[key] = match

    # If O disappeared, take the last numeric token immediately before H.
    if data["OPEN"] is None and "HIGH" in label_matches:
        prefix = text[: label_matches["HIGH"].start()]
        possible = re.findall(r"(?:[0O]?[.,]\d+|\d[\d,.]*)", prefix)
        if possible:
            data["OPEN"] = number(possible[-1])

    change_match = re.search(
        r"([+-]\s*[\d,.]+)\s*\(\s*([+-]?\s*[\d,.]+)\s*%\s*\)",
        text,
    )
    if change_match:
        data["CHANGE"] = number(change_match.group(1))
        data["CHANGE_PCT"] = number(change_match.group(2))

    score = sum(
        data[key] is not None for key in ("OPEN", "HIGH", "LOW", "CLOSE")
    )
    score += 2 * sum(
        data[key] is not None for key in ("CHANGE", "CHANGE_PCT")
    )
    return data, score


def choose_ohlc_line(lines):
    """Choose the line, or adjacent pair, most consistent with OHLC."""
    candidates = list(lines)
    for index in range(len(lines) - 1):
        first = lines[index]
        second = lines[index + 1]
        candidates.append(
            {
                "text": f'{first["text"]} {second["text"]}',
                "x1": min(first["x1"], second["x1"]),
                "x2": max(first["x2"], second["x2"]),
                "y1": min(first["y1"], second["y1"]),
                "y2": max(first["y2"], second["y2"]),
                "confidence": min(first["confidence"], second["confidence"]),
            }
        )

    best = None
    for candidate in candidates:
        parsed, score = parse_ohlc_candidate(candidate["text"])
        if best is None or score > best["score"]:
            best = {"line": candidate, "data": parsed, "score": score}
    return best


def choose_sma_line(lines, ohlc_line=None):
    """Find MA Ribbon without relying on a fixed vertical position."""
    best = None
    for line in lines:
        upper = line["text"].upper()
        score = 0
        if "RIBBON" in upper:
            score += 5
        if "SMA" in upper:
            score += 3
        if re.search(r"(?:SMA\s*,?\s*)?20\b", upper):
            score += 1
        if re.search(r"(?:SMA\s*,?\s*)?40\b", upper):
            score += 1
        if ohlc_line and line["y1"] >= ohlc_line["y1"]:
            score += 1
        if best is None or score > best["score"]:
            best = {"line": line, "score": score}
    return best if best and best["score"] >= 3 else None


def crop_detected_line(image, line, width_limit=0.82):
    height, width = image.shape[:2]
    line_height = max(12, line["y2"] - line["y1"])
    y1 = max(0, int(line["y1"] - line_height * 0.65))
    y2 = min(height, int(line["y2"] + line_height * 0.65))
    x2 = min(width, int(width * width_limit))
    return image[y1:y2, 0:x2]


def validate_parsed_chart(data):
    required = [
        "OPEN", "HIGH", "LOW", "CLOSE", "CHANGE", "CHANGE_PCT",
        "SMA20", "SMA40",
    ]
    missing = [key for key in required if data[key] is None]
    if missing:
        raise ValueError(f"OCR อ่านค่าไม่ครบ: {', '.join(missing)}")

    tolerance = max(abs(data["HIGH"]), abs(data["LOW"]), 1) * 1e-6
    if data["HIGH"] + tolerance < data["LOW"]:
        raise ValueError("ข้อมูลผิดปกติ: HIGH ต่ำกว่า LOW")
    if (
        data["OPEN"] > data["HIGH"] + tolerance
        or data["OPEN"] < data["LOW"] - tolerance
    ):
        raise ValueError("ข้อมูลผิดปกติ: OPEN อยู่นอกช่วง HIGH-LOW")
    if (
        data["CLOSE"] > data["HIGH"] + tolerance
        or data["CLOSE"] < data["LOW"] - tolerance
    ):
        raise ValueError("ข้อมูลผิดปกติ: CLOSE อยู่นอกช่วง HIGH-LOW")
    if data["SMA20"] <= 0 or data["SMA40"] <= 0:
        raise ValueError("ข้อมูลผิดปกติ: SMA ต้องมากกว่า 0")


def parse_chart_data(reader, image_bytes):
    """Hybrid adaptive OCR for TradingView headers with moving legends."""
    image = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("เปิดไฟล์ภาพไม่ได้")

    height, width = image.shape[:2]
    # Read only the header zone, avoiding candles and the right price axis.
    top_roi = image[0 : max(120, int(height * 0.25)), 0 : int(width * 0.90)]
    lines = read_positioned_lines(reader, top_roi, scale=1.0)
    best_ohlc = choose_ohlc_line(lines)

    # Retry the same region enlarged if the first pass is incomplete.
    if best_ohlc is None or best_ohlc["score"] < 8:
        enlarged_lines = read_positioned_lines(reader, top_roi, scale=1.6)
        enlarged_best = choose_ohlc_line(enlarged_lines)
        if enlarged_best and (
            best_ohlc is None or enlarged_best["score"] > best_ohlc["score"]
        ):
            lines = enlarged_lines
            best_ohlc = enlarged_best

    if best_ohlc is None:
        raise ValueError("OCR ไม่พบบรรทัด OHLC")

    data = best_ohlc["data"]
    ohlc_line = best_ohlc["line"]
    sma_choice = choose_sma_line(lines, ohlc_line)

    print(f'OCR OHLC: {ohlc_line["text"]}')
    print("OCR lines:")
    for line in lines:
        print(f'  y={line["y1"]:.0f}-{line["y2"]:.0f}: {line["text"]}')

    if sma_choice:
        sma_line = sma_choice["line"]
        sma_text = sma_line["text"]
        sma_roi = crop_detected_line(top_roi, sma_line)
    else:
        # If the label is damaged, search directly below the OHLC line.
        y1 = min(top_roi.shape[0], int(ohlc_line["y2"] + 2))
        y2 = min(top_roi.shape[0], y1 + max(50, int(height * 0.08)))
        sma_roi = top_roi[y1:y2, 0 : int(top_roi.shape[1] * 0.90)]
        sma_text = ocr_text(reader, sma_roi)

    print(f"OCR SMA : {sma_text}")
    if sma_roi.size == 0:
        raise ValueError("OCR ไม่พบพื้นที่บรรทัด SMA")

    hsv_sma = cv2.cvtColor(sma_roi, cv2.COLOR_BGR2HSV)
    data["SMA20"] = extract_colored_number(
        hsv_sma, reader, np.array([5, 100, 100]), np.array([30, 255, 255])
    )
    data["SMA40"] = extract_colored_number(
        hsv_sma, reader, np.array([80, 90, 90]), np.array([110, 255, 255])
    )

    # Text fallback: first two values following the SMA-200 legend.
    if data["SMA20"] is None or data["SMA40"] is None:
        tail = re.split(r"SMA\s*,?\s*200\)?", sma_text, maxsplit=1, flags=re.I)
        values = (
            re.findall(r"(?:[0O]?[.,]\d+|\d[\d,.]*)", tail[-1])
            if len(tail) == 2
            else []
        )
        if len(values) >= 2:
            data["SMA20"] = data["SMA20"] or number(values[0])
            data["SMA40"] = data["SMA40"] or number(values[1])

    validate_parsed_chart(data)
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
    # Google Sheets stores dates as day serials. Sending a number avoids
    # dd/mm vs mm/dd locale ambiguity while keeping the cell a real DATE.
    date_serial = (date_object.date() - date(1899, 12, 30)).days
    print(f"พบโฟลเดอร์ล่าสุด: {folder_date}")

    query = f"'{folder_id}' in parents and mimeType contains 'image/' and trashed = false"
    files = (
        drive_service.files()
        .list(q=query, fields="files(id,name)")
        .execute()
        .get("files", [])
    )

    processed = 0
    failures = []
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
            existing_values = worksheet.get(f"C{existing_row}:J{existing_row}")
            existing_values = existing_values[0] if existing_values else []
            is_complete = len(existing_values) == 8 and all(
                value not in (None, "") for value in existing_values
            )
            if is_complete:
                print(
                    f"ข้าม {asset}: มีข้อมูลครบของวันที่ {display_date} "
                    f"ที่แถว {existing_row} แล้ว"
                )
                continue
            print(
                f"ซ่อม {asset}: วันที่ {display_date} แถว {existing_row} "
                "ยังมีข้อมูลว่าง"
            )

        print(f"กำลังอ่าน {asset}: {filename}")
        try:
            parsed = parse_chart_data(
                reader, download_image_as_bytes(drive_service, file["id"])
            )
        except Exception as exc:
            failures.append((asset, filename, str(exc)))
            print(f"::warning title=OCR skipped::{asset} ({filename}): {exc}")
            continue

        row = [
            date_serial,
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
        if existing_row:
            worksheet.update(
                values=[row],
                range_name=f"A{existing_row}:J{existing_row}",
                value_input_option="USER_ENTERED",
            )
            new_row = existing_row
        else:
            worksheet.append_row(row, value_input_option="USER_ENTERED")
            new_row = len(worksheet.col_values(1))
        worksheet.format(
            f"A{new_row}", {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}}
        )
        processed += 1
        print(f"บันทึก {asset}: {row}")

    print(f"เสร็จสิ้น: บันทึก {processed} รายการ")
    if failures:
        print(f"⚠️ ข้ามรูปที่อ่านไม่สำเร็จ {len(failures)} รายการ:")
        for asset, filename, reason in failures:
            print(f"  - {asset} | {filename} | {reason}")


if __name__ == "__main__":
    main()
