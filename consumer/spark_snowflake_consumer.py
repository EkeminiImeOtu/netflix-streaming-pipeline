from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType
import sys
sys.path.append('/home/ubuntu/streaming-pipeline/snowflake')
from sink import write_to_snowflake

spark = SparkSession.builder \
    .appName("NetflixStreamingToSnowflake") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint2") \
    .config("spark.driver.memory", "512m") \
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

print("Starting Spark Streaming → Snowflake pipeline...")
print("Reading from Kafka topic: netflix-views")
print("Writing to Snowflake: PROD.DBT_RAW.NETFLIX_STREAMING_EVENTS\n")

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
    .foreachBatch(write_to_snowflake) \
    .trigger(processingTime="15 seconds") \
    .start()

print("Pipeline running! Data flowing into Snowflake every 15 seconds...")
query.awaitTermination()
