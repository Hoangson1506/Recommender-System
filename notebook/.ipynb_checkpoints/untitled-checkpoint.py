from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("ALSOfflineTrain") \
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()


# đọc từ HDFS (có thể local khi dev)
ratings = spark.read.csv("hdfs:///user/moviedata/ratings.csv", header=True, inferSchema=True)

# chuẩn hoá cột tên
ratings = ratings.selectExpr("userId as user_id", "movieId as movie_id", "rating as rating")

# split
train, test = ratings.randomSplit([0.8, 0.2], seed=42)

als = ALS(
    userCol="user_id", itemCol="movie_id", ratingCol="rating",
    rank=20, maxIter=10, regParam=0.1, coldStartStrategy="drop"
)

model = als.fit(train)

predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("RMSE:", rmse)

# Save model to HDFS
model_path = "hdfs:///user/models/movie_als_v1"
model.write().overwrite().save(model_path)
spark.stop()