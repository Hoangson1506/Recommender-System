from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import os

def main():
    spark = SparkSession.builder \
        .appName("KafkaStreamIngest") \
        .master(os.environ.get("SPARK_MASTER", "local[*]")) \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()
    
    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "user_events") \
        .load()
    
    events_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = events_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/data/realtime_ratings") \
        .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/ratings_stream") \
        .start()
    
    print("Streaming job started. Awaiting termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()