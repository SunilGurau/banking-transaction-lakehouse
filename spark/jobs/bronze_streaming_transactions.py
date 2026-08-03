from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# --- Config read from environment, with the same defaults docker-compose uses ---
# BUG FIX: both this script and silver_cleaning.py used to hardcode
# "minioadminpassword" as the S3A secret key, but docker-compose's MinIO
# service default is MINIO_ROOT_PASSWORD=minioadmin123 -- a mismatch that
# would 403 every write to MinIO. Reading from env with matching defaults
# means there's exactly one place these can drift out of sync (the
# docker-compose environment: block), not two hardcoded copies.
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")

spark = SparkSession.builder \
    .appName("Bronze_Streaming_Transactions") \
    .config(
        "spark.jars.packages",
        # BUG FIX: was spark-sql-kafka-0-10_2.13 / delta-spark_2.13 -- pip
        # installed pyspark ships a Scala 2.12 runtime, so _2.13 artifacts
        # are binary-incompatible with it. That's the exact
        # NoSuchMethodError: 'scala.collection.immutable.Seq$...' you hit.
        # Also pinned the kafka connector to 3.5.1 to match the pinned
        # pyspark==3.5.1 in the Dockerfile -- unpinned pyspark + a hardcoded
        # exact-version Maven coordinate is a version-drift trap.
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "io.delta:delta-spark_2.12:3.1.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema matching the JSONL generator output
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("fee_amount", DoubleType(), True),
    StructField("transaction_type_code", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_ts", StringType(), True),  # kept as string; cast below
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "banking.transactions.raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON from the Kafka 'value' column
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# FIX: event_ts was left as a raw string, which meant nothing downstream
# (dedup watermarks, time-based partitioning) could actually use it as time.
# Also add ingestion_date so Bronze isn't one giant unpartitioned table.
enriched_df = (
    parsed_df
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("ingestion_date", to_date(current_timestamp()))
)

# Write to MinIO Bronze Layer
query = enriched_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://bronze/_checkpoints/streaming_events/") \
    .partitionBy("ingestion_date") \
    .trigger(processingTime="30 seconds") \
    .start("s3a://bronze/transaction_events/")

query.awaitTermination()