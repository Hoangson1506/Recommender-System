from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import pandas as pd
import numpy as np
import os

spark = SparkSession.builder \
    .appName("PrepareData") \
    .master(os.environ.get("SPARK_MASTER", "local[*]")) \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

RECOMMENDATIONS_PATH = "hdfs://namenode:9000/recommendations/batch_top5"

USER_IDS_PATH = "/home/jovyan/work/data/userIds.npy"
MOVIE_IDS_PATH = "/home/jovyan/work/data/movieIds.npy"

def load_users(recommendations_path):
    user_recs = spark.read.parquet(recommendations_path)
    user_recs.printSchema()
    
    recs_exp = (
        user_recs
        .withColumn("rec", explode(col("recommendations")))
        .select(
            col("user_id"),
            col("rec.movie_id").alias("movie_id"),
            col("rec.rating").alias("score")
        )
    )
    return recs_exp.toPandas()

recommendations_pd = load_users(RECOMMENDATIONS_PATH)

user_ids = recommendations_pd['user_id'].astype(str).unique()
movie_ids = recommendations_pd['movie_id'].astype(str).unique()

np.save(USER_IDS_PATH, user_ids)
np.save(MOVIE_IDS_PATH, movie_ids)

spark.stop()