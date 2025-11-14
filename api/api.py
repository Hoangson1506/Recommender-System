import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.types import StructType, StructField, IntegerType
import redis
from kafka import KafkaProducer
import json

# Environment
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python'

RECOMMENDATION_PATH = "hdfs://namenode:9000/recommendations/batch_top5"

app = FastAPI(title="Movie Recommendation API")

spark = None
redis_client = None
kafka_producer = None
recommendations_pd = None

def get_spark_session():
    global spark
    if spark is None:
        print("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("RecommendationAPI") \
            .master("spark://spark-master:7077") \
            .config("spark.executor.cores", "6") \
            .config("spark.driver.cores", "6") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
    return spark

def get_redis_client():
    global redis_client
    if redis_client is None:
        try:
            print("Connecting to Redis...")
            redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
            redis_client.ping()
            print("Redis client connected successfully.")
        except Exception as e:
            print(f"CRITICAL: Error connecting to Redis: {e}")
            redis_client = None
    return redis_client

def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        try:
            print("Connecting to Kafka producer...")
            kafka_producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connected successfully.")
        except Exception as e:
            print(f"CRITICAL: Error connecting to Kafka: {e}")
            kafka_producer = None
    return kafka_producer

def get_recommendations():
    global recommendations_pd
    if recommendations_pd is None:
        try:
            print("Loading recommendations")
            user_recs = spark.read.parquet(RECOMMENDATION_PATH)
            
            recs_exp = (
                user_recs
                .withColumn("rec", explode(col("recommendations")))
                .select(
                    col("user_id"),
                    col("rec.movie_id").alias("movie_id"),
                    col("rec.rating").alias("score")
                )
            )

            recommendations_pd =  recs_exp.toPandas()
            print("Recommendations loaded successfully")
        except Exception as e:
            print(f"Error loading recommendations: {e}")
    return recommendations_pd

class RatingInput(BaseModel):
    user_id: int
    movieId: int
    rating: float

@app.post("/rate")
async def user_rate(data: RatingInput):
    event = {
        'user_id': data.user_id,
        'movieId': data.movieId,
        'rating': data.rating,
        'timestamp': datetime.now().isoformat()
    }

    producer = get_kafka_producer()
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer is not available.")

    try:
        producer.send('user_events', event)
        producer.flush()
        return {
            "status": "success",
            "message": "Rating event sent to Kafka.",
            "event_sent": event
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send to Kafka: {e}")

@app.get("/recommend/{user_id}")
async def recommend(user_id: int):
    r_client = get_redis_client()

    if r_client is None:
        raise HTTPException(status_code=503, detail="Redis cache is not available.")

    try:
        seen_key = f"user_seen:{user_id}"
        recently_seen_movies = r_client.smembers(seen_key)
        seen_set = {int(movie_id) for movie_id in recently_seen_movies}
        print(f"[API] User {user_id} has seen: {seen_set}")

        user_recs = recommendations_pd[recommendations_pd['user_id'] == user_id]
        recs_list = user_recs.sort_values("score", ascending=False)['movie_id']
        
        filtered_recs = []
        for rec in recs_list:
            if rec not in seen_set:
                filtered_recs.append(rec)
        
        final_recs = filtered_recs[:3]
        
        return {
            "user_id": user_id,
            "recommendations": final_recs,
            "filtered_count": len(recs_list) - len(final_recs)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def startup_event():
    get_spark_session()
    get_redis_client()
    get_kafka_producer()
    get_recommendations()
    print("API started successfully.")
