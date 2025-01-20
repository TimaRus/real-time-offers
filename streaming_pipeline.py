from datetime import datetime

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions import from_json, col, concat, concat_ws, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import redis
from clickhouse_driver import Client
import logging


# Настройка уровня логов для Spark
logger = logging.getLogger("py4j")
logger.setLevel(logging.WARN)

# Initialize Spark Session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("RealTime_Personalized_Tariff") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # Устанавливаем уровень логов для Spark

# Load Pre-trained Model
model_path = "/home/jovyan/work/enhanced_tariff_model"
pipeline_model = PipelineModel.load(model_path)

# Kafka Configuration
kafka_brokers = "kafka-cluster:9092"
kafka_topic = "streaming-user-registration"

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1") \
    .load()

# Define Schema for Incoming Data
outer_schema = StructType([
    StructField("schema", StructType([
        StructField("type", StringType(), True),
        StructField("optional", StringType(), True)
    ]), True),
    StructField("payload", StringType(), True)
])

inner_schema = StructType([
    StructField("id", StringType(), True),
    StructField("ip_country", StringType(), True),
    StructField("device_country", StringType(), True),
    StructField("device_language", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("platform_store", StringType(), True),
    StructField("smartphone_brand", StringType(), True),
    StructField("is_organic", IntegerType(), True),
    StructField("child_age", IntegerType(), True),
    StructField("child_smartphone_brand", StringType(), True)
])

parsed_outer = raw_stream.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), outer_schema).alias("data")) \
    .select("data.*")

parsed_inner = parsed_outer.select(from_json(col("payload"), inner_schema).alias("payload_data")) \
    .select("payload_data.*") \
    .withColumn("id", col("id").cast(IntegerType())) \
    .na.drop()

# Apply Pre-trained ML Model
predictions = pipeline_model.transform(parsed_inner)

# Process and Format Predictions
results = predictions.withColumn(
    "selected_functions",
    concat_ws(", ",
              when(col("predicted_function_map") > 0.5, "10 мест на карте вместо 3").otherwise(None),
              when(col("predicted_function_ignore_sound") > 0.5, "Игнорирование режима 'без звука'").otherwise(None),
              when(col("predicted_function_listen_sound") > 0.5, "Послушать звук вокруг ребенка").otherwise(None),
              when(col("predicted_function_detailed_routes") > 0.5, "Подробная карта маршрутов").otherwise(None),
              when(col("predicted_function_sos_signal") > 0.5, "Сигнал SOS").otherwise(None),
              when(col("predicted_function_invite_second_parent") > 0.5, "Приглашение второго родителя").otherwise(None)
              )
).withColumn(
    "tariff_description",
    when(
        col("selected_functions").isNull() | (col("selected_functions") == ""),
        concat(
            lit("Обязательные функции: Блокировка приложений, Защита от незнакомых звонков, Время в приложениях. Дополнительные функции: "),
            lit("нет"))
    ).otherwise(
        concat(
            lit("Обязательные функции: Блокировка приложений, Защита от незнакомых звонков, Время в приложениях. Дополнительные функции: "),
            col("selected_functions"))
    )
)

real_time_tariffs = results.select(
    "id",
    "predicted_cost",
    "predicted_subscription",
    "predicted_function_map",
    "predicted_function_ignore_sound",
    "predicted_function_listen_sound",
    "predicted_function_detailed_routes",
    "predicted_function_sos_signal",
    "predicted_function_invite_second_parent"#,
    #"selected_functions",
    #"tariff_description"
)

# Redis and ClickHouse Integration
redis_client = redis.StrictRedis(host='redis', port=6379, decode_responses=True)
clickhouse_client = Client(host='clickhouse', port=9000)


#def write_to_redis(df, batch_id):
#    try:
#        data = df.to_dict(orient='records')
#        for row in data:
#            # Добавляем текущую временную метку
#            row['write_time'] = int(time.time())  # Unix timestamp
#            # Сохраняем данные в Redis
#            redis_client.set(f"{row['id']}", str(row))
#        print(f"Batch {batch_id} written to Redis.")
#    except Exception as e:
#        print(f"Error writing to Redis: {e}")

from random import random
def write_to_redis(df, batch_id):
    try:
        data = df.to_dict(orient='records')
        for row in data:
            # Добавляем текущую временную метку в формате datetime
            row['write_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            # Генерируем уникальный ключ с префиксом "1010"
            unique_id = f"{random():.6f}".replace('.', '')  # Убираем точку для уникальности
            key = f"1010-{unique_id}"
            # Сохраняем данные в Redis
            redis_client.set(key, str(row))
        print(f"Batch {batch_id} written to Redis.")
    except Exception as e:
        print(f"Error writing to Redis: {e}")


def write_to_clickhouse(df, batch_id):
    try:
        data = df.to_dict('records')
        clickhouse_client.execute('INSERT INTO real_time_tariffs VALUES', data)
        print(f"Batch {batch_id} written to ClickHouse.")
    except Exception as e:
        print(f"Error writing to ClickHouse: {e}")


def process_and_store(batch_df, batch_id):
    try:
        df = batch_df.toPandas()
        if df.empty:
            print(f"Batch {batch_id} is empty.")
        else:
            write_to_redis(df, batch_id)
            write_to_clickhouse(df, batch_id)
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")


real_time_tariffs.writeStream \
    .foreachBatch(process_and_store) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
