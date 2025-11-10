import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import explode, col
import os


spark = SparkSession.builder \
    .appName("RecommenderDemo") \
    .master(os.environ.get("SPARK_MASTER", "local[*]")) \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "12") \
    .getOrCreate()


RECOMMENDATION_PATH = "hdfs://namenode:9000/recommendation/batch_top3"

@st.cache_data(show_spinner=False)
def load_recommendations():
    user_recs = spark.read.parquet(RECOMMENDATION_PATH)
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

recommendations_pd = load_recommendations()


st.title("Recommender System Demo")
st.markdown("Select a user to see top-N recommendations:")

user_ids = recommendations_pd['user_id'].unique()
selected_user = st.selectbox("Choose User ID", user_ids)

# Filter recommendations for selected user
user_recs = recommendations_pd[recommendations_pd['user_id'] == selected_user]

# Top-N selector
top_n = st.slider("Number of recommendations to show", min_value=1, max_value=20, value=10)
top_recs = user_recs.sort_values("score", ascending=False).head(top_n)

# Display
st.subheader(f"Top {top_n} recommendations for User {selected_user}")
st.table(top_recs)
