"""
PySpark Structured Streaming: Kafka → Delta Lake (local) → S3 Gold.
Requires: Kafka running, Delta Lake jars, AWS credentials configured.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import boto3

BROKER     = "localhost:9092"
TOPIC      = "transactions"
DELTA_PATH = "/tmp/delta/transactions"
CHECKPOINT = "/tmp/checkpoints/transactions"
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "rtpipeline-gold")

SCHEMA = StructType([
    StructField("id",         StringType()),
    StructField("user_id",    StringType()),
    StructField("category",   StringType()),
    StructField("region",     StringType()),
    StructField("amount",     DoubleType()),
    StructField("event_type", StringType()),
    StructField("timestamp",  StringType()),
])


def build_spark():
    return (SparkSession.builder
            .appName("RealtimePipeline")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def upload_to_s3(local_path: str, bucket: str, prefix: str):
    s3 = boto3.client("s3")
    import os
    for root, _, files in os.walk(local_path):
        for file in files:
            fp = os.path.join(root, file)
            key = prefix + fp.replace(local_path, "").replace("\\", "/")
            s3.upload_file(fp, bucket, key)


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BROKER)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
              .select("d.*")
              .withColumn("timestamp", F.to_timestamp("timestamp"))
              .withColumn("event_date", F.to_date("timestamp"))
              .withColumn("is_anomaly", F.col("amount") > 3000))

    def process_batch(df, epoch_id):
        if df.count() == 0:
            return
        (df.write.format("delta").mode("append")
           .partitionBy("event_date").save(DELTA_PATH))
        upload_to_s3(DELTA_PATH, GOLD_BUCKET, "delta/")
        print(f"Epoch {epoch_id}: {df.count()} rows written")

    query = (parsed.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime="30 seconds")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    run()
