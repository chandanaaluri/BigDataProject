# src/spark_streaming_consumer.py
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StringType, DoubleType

# --- Config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "agri-sensors")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "agri_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sensor_readings")

print("Starting Spark consumer")
print("Kafka bootstrap:", KAFKA_BOOTSTRAP)
print("Kafka topic:", KAFKA_TOPIC)

# --- Spark session ---
spark = (SparkSession.builder
         .appName("AgriStream")
         .master("local[*]")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# --- schema ---
schema = (StructType()
          .add("sensor_id", StringType())
          .add("ts", StringType())
          .add("soil_moisture", DoubleType())
          .add("temperature", DoubleType())
          .add("humidity", DoubleType())
          .add("rainfall", DoubleType())
         )

# --- read from Kafka ---
kafka_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")   # for tests; change for production
            .load())

# value is bytes -> cast to string and parse JSON
parsed = (kafka_df.selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), schema).alias("d"))
          .select("d.*"))

# convert ts and compute irrigation_needed
df = parsed.withColumn("ts", to_timestamp("ts"))
df = df.withColumn("irrigation_needed", expr("soil_moisture < 30 AND rainfall < 0.5"))

def write_to_mongo(batch_df, batch_id):
    # called per micro-batch
    print(f"[foreachBatch] batch_id={batch_id} rows={batch_df.count()}")
    if batch_df.rdd.isEmpty():
        print("[foreachBatch] empty batch -> skip")
        return
    # collect rows (small demo only). For larger scale use write to Mongo via connector.
    docs = [r.asDict() for r in batch_df.collect()]
    if not docs:
        print("[foreachBatch] no docs to insert")
        return
    try:
        import pymongo
        client = pymongo.MongoClient(MONGO_URI)
        col = client[MONGO_DB][MONGO_COLLECTION]
        col.insert_many(docs)
        client.close()
        print(f"[foreachBatch] inserted {len(docs)} docs into Mongo")
    except Exception as e:
        print("[foreachBatch] Mongo insert error:", e)

# start streaming query
query = (df.writeStream
         .foreachBatch(write_to_mongo)
         .outputMode("append")
         .option("checkpointLocation", "checkpoints/spark_stream")
         .start())

print("Spark streaming started - awaiting termination")
query.awaitTermination()
