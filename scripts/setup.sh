#!/bin/sh

# Ожидаем, пока Kafka Connect полностью инициализируется
echo "Waiting for Kafka Connect to initialize..."
sleep 30

# Проверяем, доступен ли Kafka Connect
until curl -s http://kafka-cluster:8083/connectors > /dev/null; do
  echo "Kafka Connect is not ready yet, waiting..."
  sleep 10
done
echo "Kafka Connect is ready!"

# Создаем топик Kafka
echo "Creating topic 'streaming-user-registration'..."
kafka-topics --create --topic streaming-user-registration --partitions 3 --replication-factor 1 --bootstrap-server kafka-cluster:9092

# Создаем Kafka Source Connector
echo "Creating FileStreamSourceConnector..."
curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" \
-d '{
  "name": "FileStreamSourceConnector",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "tasks.max": "1",
    "file": "/datasets/user-registration-data.txt",
    "topic": "streaming-user-registration",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": true,
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": true
  }
}' \
http://kafka-cluster:8083/connectors

echo "All tasks are finished"