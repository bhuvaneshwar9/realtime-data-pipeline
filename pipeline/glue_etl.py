"""AWS Glue PySpark ETL: S3 raw CSV → cleaned → gold aggregates."""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "GOLD_BUCKET"])
sc   = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job  = Job(glue)
job.init(args["JOB_NAME"], args)

RAW  = f"s3://{args['RAW_BUCKET']}/events/"
GOLD = f"s3://{args['GOLD_BUCKET']}/"

# Read & clean
df = (spark.read.option("header", True).option("inferSchema", True).csv(RAW)
      .dropDuplicates(["id"])
      .dropna(subset=["id", "user_id", "timestamp"])
      .withColumn("timestamp", F.to_timestamp("timestamp"))
      .withColumn("amount", F.col("amount").cast("double"))
      .filter(F.col("amount") > 0)
      .withColumn("event_date", F.to_date("timestamp")))

# Anomaly flagging
stats   = df.agg(F.avg("amount").alias("m"), F.stddev("amount").alias("s")).collect()[0]
df = df.withColumn("is_anomaly", F.abs(F.col("amount") - stats["m"]) > 3 * stats["s"])

# Gold: daily aggregates
daily = (df.groupBy("event_date", "region", "category")
           .agg(
               F.count("id").alias("total_events"),
               F.sum("amount").alias("total_revenue"),
               F.avg("amount").alias("avg_order"),
               F.countDistinct("user_id").alias("unique_users"),
               F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
           ))
daily.write.mode("overwrite").partitionBy("event_date").parquet(f"{GOLD}daily/")

# Gold: user summary
users = (df.groupBy("user_id")
           .agg(
               F.count("id").alias("total_orders"),
               F.sum("amount").alias("lifetime_value"),
               F.avg("amount").alias("avg_order"),
               F.max("timestamp").alias("last_seen"),
           ))
users.write.mode("overwrite").parquet(f"{GOLD}users/")

print(f"Daily rows: {daily.count()} | User rows: {users.count()}")
job.commit()
