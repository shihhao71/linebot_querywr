import os
import time
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from datetime import datetime, timezone

# === 基本設定 ===
WATCH_FOLDER = "D:/querywr"
SERVICE_ACCOUNT_FILE = os.path.join(WATCH_FOLDER, "service_account.json")
PARENT_FOLDER_ID = '10yZP68Cl6vYM0w8YFVj87UAQbKa8qSKY'
TARGET_FOLDER_ID ='1PRZXaUXxSe_YB7dJyY-zpSAK0kHRA_mJ'

RECORD_FILE = os.path.join(WATCH_FOLDER, "sync_records.json")

# === 初始化 Google Drive API ===
SCOPES = ['https://www.googleapis.com/auth/drive']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

# === 輔助函式 ===
def load_records():
    if os.path.exists(RECORD_FILE):
        with open(RECORD_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_records(records):
    with open(RECORD_FILE, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

def append_status_log(message):
    path = os.path.join(WATCH_FOLDER, 'upload_status.log')
    with open(path, 'a', encoding='utf-8') as f:
        f.write(message + '\n')

def upload_excel_file(filepath):
    file_metadata = {
        'name': os.path.basename(filepath)
    }
    if PARENT_FOLDER_ID:
        file_metadata['parents'] = [PARENT_FOLDER_ID]

    media = MediaFileUpload(
        filepath,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    return file.get('id')

def delete_google_file(file_id):
    try:
        drive_service.files().delete(fileId=file_id).execute()
        print(f"🗑 已刪除 Google Drive 檔案: {file_id}")
    except Exception as e:
        print(f"⚠ 刪除檔案時發生錯誤: {e}")

# === 監控處理器 ===
class ExcelHandler(FileSystemEventHandler):
    def __init__(self):
        self.records = load_records()

    def safe_upload(self, event_path, filename):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                with open(event_path, 'rb') as f:
                    f.read(1)
                break
            except Exception:
                print(f"⏳ 檔案尚未可讀，稍後重試... ({attempt+1}/{max_retries})")
                time.sleep(1)
        else:
            msg = f"{datetime.now(timezone.utc).isoformat()} | {filename} | FAILED | File not accessible after retries"
            append_status_log(msg)
            print(f"❌ 無法存取檔案: {event_path}")
            return

        try:
            if filename in self.records:
                old_id = self.records[filename]
                delete_google_file(old_id)

            file_id = upload_excel_file(event_path)
            self.records[filename] = file_id
            save_records(self.records)

            msg = f"{datetime.now(timezone.utc).isoformat()} | {filename} | SUCCESS | ID: {file_id}"
            append_status_log(msg)
            print(f"✅ 已成功上傳 Excel 檔案: {file_id}")

        except Exception as e:
            msg = f"{datetime.now(timezone.utc).isoformat()} | {filename} | FAILED | {str(e)}"
            append_status_log(msg)
            print(f"⚠ 上傳時發生錯誤: {e}")

    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.xlsx'):
            filename = os.path.basename(event.src_path)
            print(f"📂 偵測到新增或更新檔案：{event.src_path}")
            self.safe_upload(event.src_path, filename)

    def on_modified(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.xlsx'):
            filename = os.path.basename(event.src_path)
            print(f"📝 偵測到檔案修改：{event.src_path}")
            self.safe_upload(event.src_path, filename)

    def on_deleted(self, event):
        if event.is_directory:
            return
        filename = os.path.basename(event.src_path)
        if filename in self.records:
            file_id = self.records[filename]
            delete_google_file(file_id)
            del self.records[filename]
            save_records(self.records)
            msg = f"{datetime.now(timezone.utc).isoformat()} | {filename} | DELETE_SYNCED | ID: {file_id}"
            append_status_log(msg)
            print(f"🗑 已同步刪除 Google Drive 檔案: {file_id}")

# === 啟動監控 ===
if __name__ == "__main__":
    event_handler = ExcelHandler()
    observer = Observer()
    observer.schedule(event_handler, path=WATCH_FOLDER, recursive=False)
    observer.start()
    print(f"👀 正在監控資料夾: {WATCH_FOLDER}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
