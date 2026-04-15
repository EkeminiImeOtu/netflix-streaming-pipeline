from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType

spark = SparkSession.builder \
    .appName("NetflixStreamingConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .master("local[1]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType()),
    StructField("show_name", StringType()),
    StructField("device", StringType()),
    StructField("country", StringType()),
    StructField("watch_duration_mins", IntegerType()),
    StructField("rating", FloatType()),
    StructField("timestamp", StringType()),
    StructField("is_completed", BooleanType())
])

print("Starting Spark Streaming consumer...")
print("Reading from Kafka topic: netflix-views\n")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "netflix-views") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Spark is now consuming from Kafka...")
print("Every 10 seconds you will see a batch of events\n")

query.awaitTermination()
