from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("AgriStream").master("local[*]").getOrCreate()

schema = StructType()     .add("sensor_id", StringType())     .add("ts", StringType())     .add("soil_moisture", DoubleType())     .add("temperature", DoubleType())     .add("humidity", DoubleType())     .add("rainfall", DoubleType())

kafka_df = spark.readStream.format("kafka")     .option("kafka.bootstrap.servers","localhost:9092")     .option("subscribe","agri-sensors")     .load()

parsed = kafka_df.selectExpr("CAST(value AS STRING) AS json")     .select(from_json(col("json"), schema).alias("d")).select("d.*")

df = parsed.withColumn("ts", to_timestamp("ts"))
df = df.withColumn("irrigation_needed",
                   expr("soil_moisture < 30 AND rainfall < 0.5"))

def write_to_mongo(batch_df, batch_id):
    import pymongo
    client = pymongo.MongoClient("mongodb://localhost:27017")
    col = client["agri_db"]["sensor_readings"]
    docs = [r.asDict() for r in batch_df.collect()]
    if docs:
        col.insert_many(docs)
    client.close()

df.writeStream.foreachBatch(write_to_mongo).start()
spark.streams.awaitAnyTermination()
