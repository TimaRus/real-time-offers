import json
import time
from datetime import datetime

import requests
from tqdm import tqdm

# URL Kafka REST Proxy
KAFKA_URL = "http://127.0.0.1:8082/topics/streaming-user-registration"

# Заголовки для Content-Type
HEADERS = {
    "Content-Type": "application/vnd.kafka.json.v2+json",
    "Accept": "application/vnd.kafka.v2+json"
}


def create_record(user_id: int):
    """Создание записи для отправки в Kafka."""
    # Текущее время в формате ISO 8601
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "records": [
            {
                "key": f"{user_id}",
                "value": {
                    "schema": {
                        "type": "string",
                        "optional": False
                    },
                    "payload": {
                        "user_id": f"{user_id}",
                        "ip_country": "ru",
                        "device_country": "ru",
                        "device_language": "ru",
                        "platform": "Android",
                        "platform_store": "Google Play",
                        "smartphone_brand": "xiaomi",
                        "is_organic": 0,
                        "child_age": 7,
                        "child_smartphone_brand": "iphone",
                        "timestamp": timestamp
                    }
                }
            }
        ]
    }


def send_request(record):
    """Отправка запроса в Kafka."""
    try:
        response = requests.post(KAFKA_URL, headers=HEADERS, data=json.dumps(record))
        response.raise_for_status()
        print("[SUCCESS] Record sent: ", record["records"][0]["key"])
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to send record {record['records'][0]['key']}: {e}")


def main():
    print(f'start at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # Функция для отправки 1000 запросов с интервалом 250 мс
    for user_id in tqdm(range(1, 1001), desc="Sending records", unit="record"):
        record = create_record(user_id)
        send_request(record)
        time.sleep(0.250)

    print(f'finish at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')


if __name__ == "__main__":
    main()
