import os
import requests
from dotenv import load_dotenv

from script import message_text

load_dotenv()
TG_TOKEN = os.getenv("TG_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def send_message(text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    response = requests.post(url, data=payload)
    return response.json()

if __name__ == "__main__":
    result = send_message(message_text)
    print(result)