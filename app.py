import os
import json
import io
import time
from flask import Flask, request, abort
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

from linebot.v3 import WebhookHandler
from linebot.v3.messaging import MessagingApi, Configuration, ApiClient
from linebot.v3.messaging.models import TextMessage, ReplyMessageRequest
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from google.oauth2.service_account import Credentials

# === 載入 .env 變數 ===
load_dotenv()
CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

app = Flask(__name__)
configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# === Google Drive 設定 ===
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
#SERVICE_ACCOUNT_FILE = 'credentials.json'
FOLDER_ID = '1PRZXaUXxSe_YB7dJyY-zpSAK0kHRA_mJ'
ALL_DATA_PATH = 'all_data.json'
QUERY_LOG_PATH = 'query_log.json'
QUERY_HISTORY_PATH = 'query_history.json'

creds_info = {
    "type": os.getenv("GOOGLE_TYPE"),
    "project_id": os.getenv("GOOGLE_PROJECT_ID"),
    "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
    "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
    "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
    "client_id": os.getenv("GOOGLE_CLIENT_ID"),
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.getenv('GOOGLE_CLIENT_EMAIL')}"
}


# === 判斷是否應該下載 ===
def should_download():
    if not os.path.exists(ALL_DATA_PATH):
        return True
    mtime = os.path.getmtime(ALL_DATA_PATH)
    return time.time() - mtime > 600

def download_all_data_from_drive():
    #creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    query = f"'{FOLDER_ID}' in parents and name = 'all_data.json' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        raise FileNotFoundError("找不到 all_data.json")
    file_id = items[0]['id']
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(ALL_DATA_PATH, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

# === 回傳文字 ===
def reply_text(reply_token, text):
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        reply_request = ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=text)])
        line_bot_api.reply_message(reply_request)

# === 清理過期紀錄 ===
def clean_old_entries(log):
    now = datetime.now(timezone.utc)
    threshold = now - timedelta(days=7)
    return {k: v for k, v in log.items() if datetime.fromtimestamp(v['timestamp'], tz=timezone.utc) > threshold}

# === 處理訊息 ===
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except Exception as e:
        print(f"[ERROR] {e}")
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_text = event.message.text.strip()
    reply_token = event.reply_token
    user_id = event.source.user_id
    message_id = event.message.id
    now_ts = time.time()

    try:
        if should_download():
            download_all_data_from_drive()
        with open(ALL_DATA_PATH, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
    except Exception as e:
        return reply_text(reply_token, f"❌ 資料下載錯誤：{e}")

    query_log = {}
    if os.path.exists(QUERY_LOG_PATH):
        with open(QUERY_LOG_PATH, 'r', encoding='utf-8') as f:
            query_log = json.load(f)

    query_history = {}
    if os.path.exists(QUERY_HISTORY_PATH):
        with open(QUERY_HISTORY_PATH, 'r', encoding='utf-8') as f:
            query_history = json.load(f)

    query_history = clean_old_entries(query_history)
    if message_id in query_history:
        return
    query_history[message_id] = {'timestamp': now_ts, 'user': user_id}

    # 還原功能
    if user_text.startswith("還原"):
        parts = user_text.split()
        if len(parts) == 2:
            raw = parts[1].replace("編號", "")
            found = False
            for entry in all_data:
                if entry.get("編號") == raw:
                    query_log[raw] = int(entry.get("箱數", 0))
                    found = True
                    with open(QUERY_LOG_PATH, 'w', encoding='utf-8') as f:
                        json.dump(query_log, f, ensure_ascii=False, indent=2)
                    return reply_text(reply_token, f"✅ 已還原：編號 {raw} 的箱數為 {query_log[raw]}")
            if not found:
                available = [e.get("編號") for e in all_data]
                msg = f"❌ 查無編號 {raw} 資料\n可用編號包含：{', '.join(available[:5])} ..."
                return reply_text(reply_token, msg)
        else:
            return reply_text(reply_token, "❌ 請輸入：還原 編號XXXX")

    # 設定功能
    if user_text.startswith("設定"):
        parts = user_text.split()
        if len(parts) == 3 and parts[2].isdigit():
            raw = parts[1].replace("編號", "")
            指定箱數 = int(parts[2])
            found = False
            for entry in all_data:
                if entry.get("編號") == raw:
                    query_log[raw] = 指定箱數
                    found = True
                    with open(QUERY_LOG_PATH, 'w', encoding='utf-8') as f:
                        json.dump(query_log, f, ensure_ascii=False, indent=2)
                    return reply_text(reply_token, f"✅ 已設定：編號 {raw} 剩餘箱數為 {指定箱數}")
            if not found:
                available = [e.get("編號") for e in all_data]
                msg = f"❌ 查無編號 {raw} 資料\n可用編號包含：{', '.join(available[:5])} ..."
                return reply_text(reply_token, msg)
        else:
            return reply_text(reply_token, "❌ 請輸入：設定 編號XXXX 數字")

    # 查詢與扣箱
    matched = []
    total_remaining = 0
    for entry in all_data:
        編號 = entry.get("編號", "")
        原始箱數 = int(entry.get("箱數", 0))
        條碼們 = [str(i.get("商品編號", "")) for i in entry.get("資料", [])] + [str(i.get("條碼", "")) for i in entry.get("資料", [])]

        if user_text in 條碼們:
            if 編號 not in query_log:
                query_log[編號] = 原始箱數
            query_log[編號] = max(0, query_log[編號] - 1)
            matched.append(f"- 編號 {編號}，原始箱數 {原始箱數}，剩餘箱數：{query_log[編號]}")
            total_remaining += query_log[編號]
            break

    with open(QUERY_LOG_PATH, 'w', encoding='utf-8') as f:
        json.dump(query_log, f, ensure_ascii=False, indent=2)
    with open(QUERY_HISTORY_PATH, 'w', encoding='utf-8') as f:
        json.dump(query_history, f, ensure_ascii=False, indent=2)

    if matched:
        msg = "✅ 查詢結果：\n" + "\n".join(matched)
        msg += f"\n➕ 總剩餘箱數：{total_remaining}"
        if total_remaining == 0:
            msg += "\n✅ 下貨完畢"
    else:
        msg = "❌ 查無資料"

    reply_text(reply_token, msg)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8886)
