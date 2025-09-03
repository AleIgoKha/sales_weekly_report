import os
import requests
from dotenv import load_dotenv

from script import message_text, image_file

load_dotenv()
TG_TOKEN = os.getenv("TG_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def send_message(image_file, caption: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto"
    payload = {"chat_id": CHAT_ID, "caption": caption}
    response = requests.post(url, data=payload, files=image_file)
    return response.json()

if __name__ == "__main__":
    result = send_message(image_file, caption=message_text)
    print(result)