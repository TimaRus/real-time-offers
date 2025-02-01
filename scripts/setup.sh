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

echo "All tasks are finished"