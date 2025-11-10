from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import os

spark = SparkSession.builder \
    .appName("MovieRecommender") \
    .master(os.environ.get("SPARK_MASTER", "local[*]")) \
    .getOrCreate()

# HDFS path to the model
model_path = "hdfs://namenode:9000/models/movie_als_v1"

# Load model
model = ALSModel.load(model_path)

user_recs = model.recommendForAllUsers(5)

# Save results
output_path = "hdfs://namenode:9000/recommendation/batch_top3"
user_recs.write.mode("overwrite").parquet(output_path)

print(f"User recommendations saved to {output_path}")

spark.stop()
