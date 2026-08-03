from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, to_timestamp, to_date
from delta.tables import DeltaTable
import os
import time

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")

# FIX: appName said "Kafka_to_Bronze" (copy-paste from the bronze script) --
# harmless but confusing in the Spark UI when both jobs are running.
spark = SparkSession.builder \
    .appName("Bronze_to_Silver_Cleaning") \
    .config(
        "spark.jars.packages",
        # Same Scala-version fix as bronze: _2.13 -> _2.12, pinned to 3.5.1
        # to match the pinned pyspark version.
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

BRONZE_PATH = "s3a://bronze/transaction_events/"


def wait_for_bronze_table(spark, path, retries=30, wait_seconds=10):
    """FIX: previously the very first spark.readStream.load() on a Bronze
    table that hasn't received its first committed batch yet threw
    DELTA_SCHEMA_NOT_SET, which killed the whole Spark job -- and because
    silver-streaming has restart: unless-stopped, Docker just kept
    relaunching it into the same wall every restart cycle. This waits
    quietly (with periodic status prints, no exception) until bronze-streaming
    has actually written something, which happens the moment kafka-producer
    starts pushing real messages through banking.transactions.raw."""
    for attempt in range(1, retries + 1):
        if DeltaTable.isDeltaTable(spark, path):
            print(f"Bronze table found at {path}, starting Silver stream.")
            return
        print(f"Waiting for bronze-streaming to write its first batch to {path} "
            f"(attempt {attempt}/{retries}, retrying in {wait_seconds}s)...")
        time.sleep(wait_seconds)
    raise RuntimeError(
        f"Bronze table at {path} still not initialized after "
        f"{retries * wait_seconds}s. Check that kafka-producer is actually "
        f"sending messages (docker logs kafka-producer) and that "
        f"bronze-streaming is running (docker logs bronze_streaming_worker)."
    )


wait_for_bronze_table(spark, BRONZE_PATH)

# Read the Bronze stream from MinIO
bronze_stream = (
    spark.readStream.format("delta")
    .load(BRONZE_PATH)
    .withColumn("event_ts", to_timestamp(col("event_ts")))  # Bronze now writes this as a real timestamp; cast defensively in case Bronze is rerun from an older checkpoint
)

# ---------------------------------------------------------
# Splitting the Stream (Good vs. Bad Data)
# ---------------------------------------------------------
malformed_stream = bronze_stream.filter(col("transaction_id").isNull() | col("account_id").isNull())

# FIX (gap, not a crash bug): the original validated_stream had no
# deduplication at all. Your capstone brief's own Silver-layer definition
# is "cleaned, deduplicated, conformed, enriched," and fact_transaction is
# supposed to be one row per unique transaction_id -- so duplicates from
# producer retries would have flowed straight through to Gold. Added a
# watermark + dropDuplicates here, which is the standard Structured
# Streaming pattern for streaming dedup.
validated_stream = (
    bronze_stream
    .filter(col("transaction_id").isNotNull() & col("account_id").isNotNull())
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["transaction_id"])
)

# FRAUD DATA: Valid, deduped records that trigger a simple risk rule
fraud_stream = validated_stream.filter(col("amount") > 10000)

# ---------------------------------------------------------
# Writing to MinIO: The DLQ Table
# ---------------------------------------------------------
print("Routing malformed records to DLQ...")
dlq_query = malformed_stream \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://silver/_checkpoints/dlq/") \
    .trigger(processingTime="30 seconds") \
    .start("s3a://silver/dlq_transactions/")

# ---------------------------------------------------------
# Writing to MinIO: The Validated Table
# ---------------------------------------------------------
# NOTE: dropDuplicates + withWatermark is a stateful operation. Because
# validated_stream feeds two independent sinks below (Delta write +
# fraud_stream -> Kafka), Spark runs the dedup state machine once per query,
# so each sink keeps its own state store under its own checkpoint. That's
# expected/supported, just means dedup work happens twice -- fine at this
# volume, worth knowing if you scale up.
print("Routing clean records to Validated table...")
validated_query = validated_stream \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://silver/_checkpoints/validated/") \
    .trigger(processingTime="30 seconds") \
    .start("s3a://silver/validated_transactions/")

# ---------------------------------------------------------
# Writing to Kafka: The Fraud Alerts Topic
# ---------------------------------------------------------
print("Routing suspicious records to Fraud Alerts topic...")
fraud_query = fraud_stream \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", "banking.fraud.alerts") \
    .option("checkpointLocation", "s3a://silver/_checkpoints/kafka_fraud/") \
    .trigger(processingTime="30 seconds") \
    .start()

# Wait for all streams to process
spark.streams.awaitAnyTermination()