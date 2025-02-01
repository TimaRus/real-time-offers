import time

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

# Запуск Spark Session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("RealTime_Personalized_Tariff") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Устанавливаем уровень логов для Spark
spark.sparkContext.setLogLevel("WARN")

# Загружаем ML модель
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

# Определяем схему для входящих данных
outer_schema = StructType([
    StructField("schema", StructType([
        StructField("type", StringType(), True),
        StructField("optional", StringType(), True)
    ]), True),
    StructField("payload", StringType(), True)
])

inner_schema = StructType([
    StructField("user_id", StringType(), True),
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
    .withColumn("user_id", col("user_id").cast(IntegerType())) \
    .na.drop()

# Применяем ML модель
tariff_predictions = pipeline_model.transform(parsed_inner)

offer_result = tariff_predictions.select(
    "user_id",
    "predicted_cost",
    "predicted_subscription",
    "predicted_function_map",
    "predicted_function_ignore_sound",
    "predicted_function_listen_sound",
    "predicted_function_detailed_routes",
    "predicted_function_sos_signal",
    "predicted_function_invite_second_parent"
)

# Подключения к Redis и ClickHouse
redis_client = redis.StrictRedis(host='redis', port=6379, decode_responses=True)
clickhouse_client = Client(host='clickhouse', port=9000)

clickhouse_client.execute('''
CREATE TABLE IF NOT EXISTS personal_offer (
    user_id UInt32,
    predicted_cost Float32,
    predicted_subscription Float32,
    predicted_function_map Float32,
    predicted_function_ignore_sound Float32,
    predicted_function_listen_sound Float32,
    predicted_function_detailed_routes Float32,
    predicted_function_sos_signal Float32,
    predicted_function_invite_second_parent Float32,
    _inserted_datetime DateTime MATERIALIZED now()
) ENGINE = MergeTree()
ORDER BY user_id;
''')

def write_to_redis(df, batch_id):
    try:
        data = df.to_dict(orient='records')
        for row in data:
            # Добавляем текущую временную метку
            row['inserted_datetime'] = int(time.time())
            # Сохраняем данные в Redis
            redis_client.set(f"{row['user_id']}", str(row))
        print(f"Batch {batch_id} written to Redis.")
    except Exception as e:
        print(f"Error writing to Redis: {e}")


def write_to_clickhouse(df, batch_id):
    try:
        data = df.to_dict('records')
        clickhouse_client.execute('INSERT INTO personal_offer VALUES', data)
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


offer_result.writeStream \
    .foreachBatch(process_and_store) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
