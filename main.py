"""
=============================================================================
🤖 TRADINGVIEW OCR & GOOGLE SHEETS AUTOMATION (GITHUB WORKFLOW VERSION)
=============================================================================
สกัดค่า OPEN, HIGH, LOW, CLOSE, CHANGE, % CHANGE, SMA20 (สีส้ม), SMA40 (สีฟ้า)
ด้วย EasyOCR + Color Masking (OpenCV) แล้วบันทึกข้อมูลลง Google Sheets
"""

import io
import os
import re
from datetime import datetime
import cv2
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import numpy as np

# โหลด EasyOCR
import easyocr

# =============================================================================
# ⚙️ CONFIGURATION & ENVIRONMENT VARIABLES
# =============================================================================
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
]


def get_config():
  """ดึงค่าการตั้งค่าจาก Environment Variables (รองรับทั้ง Local และ GitHub Secrets)"""
  return {
      'CREDENTIALS_JSON': os.getenv('GOOGLE_CREDENTIALS_JSON'),
      'PARENT_FOLDER_ID': os.getenv(
          'PARENT_FOLDER_ID', 'YOUR_GOOGLE_DRIVE_FOLDER_ID'
      ),
      'SPREADSHEET_NAME': os.getenv('SPREADSHEET_NAME', 'Market Analysis'),
  }


# =============================================================================
# 📌 1. GOOGLE DRIVE & SHEETS AUTHENTICATION
# =============================================================================
def get_authenticated_services(config):
  """ยืนยันตัวตน Google API ผ่าน Service Account"""
  creds_json = config['CREDENTIALS_JSON']

  if creds_json:
    import json

    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
  else:
    # กรณีรันบนเครื่อง Local หากไม่ได้ตั้ง Env ให้เรียกใช้ไฟล์ credentials.json
    creds = Credentials.from_service_account_file(
        'credentials.json', scopes=SCOPES
    )

  drive_service = build('drive', 'v3', credentials=creds)
  gc = gspread.authorize(creds)
  return drive_service, gc


def get_latest_date_folder(drive_service, parent_id):
  """ค้นหาโฟลเดอร์วันที่ YYYYMMDD ล่าสุดใน Google Drive"""
  query = (
      f"'{parent_id}' in parents and mimeType ="
      " 'application/vnd.google-apps.folder' and trashed = false"
  )
  results = (
      drive_service.files().list(q=query, fields='files(id, name)').execute()
  )
  folders = results.get('files', [])

  date_folders = [f for f in folders if re.match(r'^\d{8}$', f['name'])]
  if not date_folders:
    return None, None

  date_folders.sort(key=lambda x: x['name'], reverse=True)
  return date_folders[0]['name'], date_folders[0]['id']


def download_image_as_bytes(drive_service, file_id):
  """ดาวน์โหลดรูปภาพเข้า Memory โดยตรง"""
  request = drive_service.files().get_media(file_id=file_id)
  fh = io.BytesIO()
  downloader = MediaIoBaseDownload(fh, request)
  done = False
  while not done:
    _, done = downloader.next_chunk()
  fh.seek(0)
  return fh.read()


# =============================================================================
# 🎨 2. COLOR MASKING & ADVANCED OCR EXTRACTION
# =============================================================================
def extract_sma_by_color(hsv_roi, reader, lower_hsv, upper_hsv):
  """ใช้ Color Masking สกัดเฉพาะข้อความสีที่กำหนด แล้วอ่านด้วย OCR"""
  mask = cv2.inRange(hsv_roi, lower_hsv, upper_hsv)
  # ขยายขนาดภาพเล็กน้อยเพื่อให้ OCR อ่านตัวเลขชัดขึ้น
  resized_mask = cv2.resize(
      mask, (0, 0), fx=2, fy=2, interpolation=cv2.INTER_CUBIC
  )

  results = reader.readtext(resized_mask, detail=0)
  combined_text = ' '.join(results)

  # ค้นหาตัวเลขที่มีจุดทศนิยม
  match = re.search(r'([\d\.\,]+)', combined_text)
  if match:
    try:
      return float(match.group(1).replace(',', ''))
    except ValueError:
      return None
  return None


def parse_chart_data(reader, image_bytes):
  """สกัดค่า OHLC, CHANGE, CHANGE_PCT, SMA20 (เส้นส้ม) และ SMA40 (เส้นฟ้า)"""
  nparr = np.frombuffer(image_bytes, np.uint8)
  img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

  h, w, _ = img.shape
  # Crop แถบข้อมูลมุมซ้ายบน (TradingView Legend)
  roi = img[0 : int(h * 0.25), 0 : int(w * 0.65)]

  # --- อ่านข้อมูล OHLC รวมทั้งหมด ---
  ocr_results = reader.readtext(roi, detail=0)
  full_text = ' '.join(ocr_results)

  data = {
      'OPEN': None,
      'HIGH': None,
      'LOW': None,
      'CLOSE': None,
      'CHANGE': None,
      'CHANGE_PCT': None,
      'SMA20': None,
      'SMA40': None,
  }

  # Regex สกัด OHLC
  o_m = re.search(r'\bO(?:PEN)?\s*[:=]?\s*([\d\.\,]+)', full_text, re.IGNORECASE)
  h_m = re.search(r'\bH(?:IGH)?\s*[:=]?\s*([\d\.\,]+)', full_text, re.IGNORECASE)
  l_m = re.search(r'\bL(?:OW)?\s*[:=]?\s*([\d\.\,]+)', full_text, re.IGNORECASE)
  c_m = re.search(
      r'\bC(?:LOSE)?\s*[:=]?\s*([\d\.\,]+)', full_text, re.IGNORECASE
  )

  if o_m:
    data['OPEN'] = float(o_m.group(1).replace(',', ''))
  if h_m:
    data['HIGH'] = float(h_m.group(1).replace(',', ''))
  if l_m:
    data['LOW'] = float(l_m.group(1).replace(',', ''))
  if c_m:
    data['CLOSE'] = float(c_m.group(1).replace(',', ''))

  # --- สกัด SMA20 & SMA40 ผ่านแยกสี (HSV Masking) ---
  hsv_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)

  # ช่วงสีส้มสำหรับ SMA20 (TradingView Orange Line/Label)
  orange_lower = np.array([5, 120, 120])
  orange_upper = np.array([25, 255, 255])
  data['SMA20'] = extract_sma_by_color(
      hsv_roi, reader, orange_lower, orange_upper
  )

  # ช่วงสีฟ้า/Cyan สำหรับ SMA40 (TradingView Cyan Line/Label)
  cyan_lower = np.array([85, 120, 120])
  cyan_upper = np.array([105, 255, 255])
  data['SMA40'] = extract_sma_by_color(hsv_roi, reader, cyan_lower, cyan_upper)

  # Fallback: ถ้าคัดแยกสีไม่ได้ ให้สกัดจาก Text ตรงๆ
  if not data['SMA20']:
    sma20_m = re.search(
        r'SMA\s*20[^\d]*([\d\.\,]+)', full_text, re.IGNORECASE
    )
    if sma20_m:
      data['SMA20'] = float(sma20_m.group(1).replace(',', ''))

  if not data['SMA40']:
    sma40_m = re.search(
        r'SMA\s*40[^\d]*([\d\.\,]+)', full_text, re.IGNORECASE
  )
    if sma40_m:
      data['SMA40'] = float(sma40_m.group(1).replace(',', ''))

  # คำนวณ CHANGE และ CHANGE_PCT
  if data['CLOSE'] is not None and data['OPEN'] is not None:
    data['CHANGE'] = round(data['CLOSE'] - data['OPEN'], 4)
    data['CHANGE_PCT'] = (
        f'{round((data["CHANGE"] / data["OPEN"]) * 100, 2)}%'
    )

  return data


# =============================================================================
# 🚀 3. MAIN EXECUTION (ตรรกะบันทึกข้อมูลเหมือน GAS)
# =============================================================================
def main():
  print('🚀 เริ่มต้นระบบ Python OCR อ่านภาพกราฟ TradingView...')
  config = get_config()
  drive_service, gc = get_authenticated_services(config)
  spreadsheet = gc.open(config['SPREADSHEET_NAME'])

  reader = easyocr.Reader(['en'], gpu=False)

  # 1. ตรวจสอบโฟลเดอร์วันที่ล่าสุด YYYYMMDD
  folder_date_str, folder_id = get_latest_date_folder(
      drive_service, config['PARENT_FOLDER_ID']
  )
  if not folder_id:
    print('❌ ไม่พบโฟลเดอร์รูปแบบ YYYYMMDD ใน Google Drive')
    return

  print(f'📁 พบโฟลเดอร์ล่าสุด: {folder_date_str}')
  # แปลงเป็น ฟอร์แมต YYYY-MM-DD
  formatted_date = (
      f'{folder_date_str[:4]}-{folder_date_str[4:6]}-{folder_date_str[6:8]}'
  )

  # 2. ดึงไฟล์ภาพทั้งหมด
  query = (
      f"'{folder_id}' in parents and mimeType contains 'image/' and trashed ="
      ' false'
  )
  files = (
      drive_service.files()
      .list(q=query, fields='files(id, name)')
      .execute()
      .get('files', [])
  )

  if not files:
    print('⚠️ ไม่พบไฟล์ภาพในโฟลเดอร์')
    return

  processed = 0

  for file in files:
    file_name = file['name']
    asset_name = file_name.split('_')[0].upper()

    # จัดการ Sheet รายสินทรัพย์
    try:
      worksheet = spreadsheet.worksheet(asset_name)
    except gspread.exceptions.WorksheetNotFound:
      worksheet = spreadsheet.add_worksheet(
          title=asset_name, rows='1000', cols='20'
      )
      # สร้าง Header และจัดรูปแบบตัวหนา (Bold Header)
      worksheet.append_row([
          'DATE',
          'FILENAME',
          'OPEN',
          'HIGH',
          'LOW',
          'CLOSE',
          'CHANGE',
          '% CHANGE',
          'SMA20',
          'SMA40',
      ])
      worksheet.format('1:1', {'textFormat': {'bold': True}})

    # 3. ระบบตรวจสอบข้อมูลซ้ำ (Prevent Duplicate Check)
    if worksheet.row_count > 1:
      existing_dates = worksheet.col_values(1)[1:]  # ข้าม Header
      if (
          formatted_date in existing_dates
          or folder_date_str in existing_dates
      ):
        print(f'⏩ ข้าม {asset_name}: มีข้อมูลวันที่ {formatted_date} แล้ว')
        continue

    print(f'📸 กำลังอ่านภาพสินทรัพย์: {asset_name} ({file_name})...')
    img_bytes = download_image_as_bytes(drive_service, file['id'])
    parsed = parse_chart_data(reader, img_bytes)

    # 4. บันทึกข้อมูลบรรทัดใหม่ลง Google Sheets
    row_data = [
        formatted_date,
        file_name,
        parsed['OPEN'],
        parsed['HIGH'],
        parsed['LOW'],
        parsed['CLOSE'],
        parsed['CHANGE'],
        parsed['CHANGE_PCT'],
        parsed['SMA20'],
        parsed['SMA40'],
    ]

    worksheet.append_row(row_data)
    processed += 1
    print(
        f'✅ บันทึกสำเร็จ: {asset_name} | CLOSE: {parsed["CLOSE"]} | SMA20:'
        f' {parsed["SMA20"]} (ส้ม) | SMA40: {parsed["SMA40"]} (ฟ้า)'
    )

  print(f'\n🎉 เสร็จสิ้น! ประมวลผลและอัปเดตข้อมูลสำเร็จทั้งสิ้น {processed} รายการ')


if __name__ == '__main__':
  main()
