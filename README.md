# Recommender System (Big Data Project)

This project implements a recommender system designed to run in a big data environment using Docker, Apache Spark (PySpark), and HDFS.

## 📋 Prerequisites

Before you can use this repo, ensure you have the following installed on your system:
* [Docker](https://www.docker.com/get-started)
* [Docker Compose](https://docs.docker.com/compose/install/)

---

## 🚀 Getting Started: Installation & Data Setup

### 1. Start the Docker Environment

This command will build and start all the necessary services (Namenode, Datanode, Spark Master, PySpark Notebook) in detached mode.

```bash
docker compose up -d
````

### 2\. Load Data into HDFS

These commands will copy local data into the `namenode` container and then move it into HDFS.

```bash
# 1. Copy data from your local machine to the namenode container
docker cp ./data namenode:/tmp/

# 2. Create a '/data' directory in HDFS
docker exec -it namenode hdfs dfs -mkdir /data

# 3. Put the data from the container's temp folder into HDFS
docker exec -it namenode hdfs dfs -put /tmp/data/* /data
# if error encountered, use this instead:
docker exec -it namenode bash -c "hdfs dfs -put /tmp/data/* /data/"
```

You can (optionally) verify the data was loaded correctly by listing the contents of the HDFS directory:

```bash
docker exec -it namenode hdfs dfs -ls /data
```

-----

## 🛠️ User Guide: Running Jobs

All jobs are run by executing `spark-submit` commands inside the `bigdata-stack` container.

### 1\. Run Batch Training

This command executes the training script. The resulting models will be saved to HDFS.

```bash
docker exec -it bigdata-stack bash -c "cd work/spark && spark-submit batch_train.py"
```

  * **Model Output:** Models are saved to `hdfs://namenode:9000/models`

### 2\. Run Batch Recommendation

This command uses the trained models to generate recommendations for users.

```bash
docker exec -it bigdata-stack bash -c "cd work/spark && spark-submit batch_recommend.py"
```

  * **Recommendation Output:** Recommendations are saved to `hdfs://namenode:9000/recommendations`

### 3\. Prepare the data for streaming

This command will prepare the data for the streaming process.

```bash
docker exec -it bigdata-stack bash -c "cd work/spark && spark-submit prepare_data.py"
```

### 4\. Run kafka streaming simulations

These command will run a simulation of data streaming. producer.py makes synthesis user rating data and stream_ingest.py reads it then write to data/realtime_ratings in the hdfs.

```bash
# 1. Run this in a new terminal. This will make stream_ingest.py start working and wait for data from producer
docker exec bigdata-stack spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 /home/jovyan/work/kafka/stream_ingest.py   

# 2. Run this in another new terminal. This will make producer.py start working.
docker exec bigdata-stack python /home/jovyan/work/kafka/producer.py
```

You can check if the system is working correctly by listing the contents in HDFS:

```bash
docker exec namenode hdfs dfs -ls /data/realtime_ratings
```

-----

## ⚙️ Configuration Notes

  * **Spark Master:** When configuring your PySpark application, the Spark Master is available at the address specified in the `SPARK_MASTER` environment variable (within the container).
  * **HDFS Connection:** To access HDFS from Spark, use the following URL: `hdfs://namenode:9000/`

## Links you can access to manage the process
http://localhost:8080: Spark Master UI

```bash
# Run this command and find 127.0.0.1:8888/token=... to access Jupyter notebook demo at work/notebook/demo.ipynb
docker logs bigdata-stack
```