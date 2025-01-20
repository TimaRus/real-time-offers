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


def generate_current_datetime():
    """Генерация текущей даты и времени в формате YYYY-MM-DD HH:mm:ss."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_record():
    """Создание записи для отправки в Kafka."""
    current_datetime = generate_current_datetime()
    return {
        "records": [
            {
                "key": current_datetime,
                "value": {
                    "schema": {
                        "type": "string",
                        "optional": False
                    },
                    "payload": {
                        "id": "3",
                        "ip_country": "fr",
                        "device_country": "fr",
                        "device_language": "fr",
                        "platform": "Android",
                        "platform_store": "Google Play",
                        "smartphone_brand": "xiaomi",
                        "is_organic": 0,
                        "child_age": 27,
                        "child_smartphone_brand": "iphone"
                    }
                }
            }
        ]
    }


def send_request(record):
    """Отправка запроса в Kafka."""
    response = requests.post(KAFKA_URL, headers=HEADERS, data=json.dumps(record))
    if response.status_code == 200:
        print("[SUCCESS] Record sent: ", record["records"][0]["key"])
    else:
        print("[ERROR] Failed to send record:", response.status_code, response.text)


def main():
    print(f'start at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    """Основная функция для отправки 1000 запросов с интервалом 200 мс."""
    for _ in tqdm(range(1000), desc="Sending records", unit="record"):
        record = create_record()
        send_request(record)
        time.sleep(0.333)  # 250 мс

    print(f'finish at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')


if __name__ == "__main__":
    main()


# Для 5 записей в секунду: интервал между отправками должен быть 200 ms.
# Для 4 записей в секунду: интервал между отправками должен быть 250 ms
# Для 3 записей в секунду: интервал между отправками должен быть 333 ms (можно округлить).
