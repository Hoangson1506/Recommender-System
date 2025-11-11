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

All jobs are run by executing `spark-submit` commands inside the `pyspark-notebook` container.

### 1\. Run Batch Training

This command executes the training script. The resulting models will be saved to HDFS.

```bash
docker exec -it pyspark-notebook bash -c "cd work/spark && spark-submit batch_train.py"
```

  * **Model Output:** Models are saved to `hdfs://namenode:9000/models`

### 2\. Run Batch Recommendation

This command uses the trained models to generate recommendations for users.

```bash
docker exec -it pyspark-notebook bash -c "cd work/spark && spark-submit batch_recommend.py"
```

  * **Recommendation Output:** Recommendations are saved to `hdfs://namenode:9000/recommendations`

-----

## ⚙️ Configuration Notes

  * **Spark Master:** When configuring your PySpark application, the Spark Master is available at the address specified in the `SPARK_MASTER` environment variable (within the container).
  * **HDFS Connection:** To access HDFS from Spark, use the following URL: `hdfs://namenode:9000/`
