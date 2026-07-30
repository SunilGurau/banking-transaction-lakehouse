from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Added the AWS Java SDK Bundle to the packages list
# 2. Changed endpoints to localhost for local testing
spark = SparkSession.builder \
    .appName("Kafka_to_Bronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching your JSONL generator output
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("fee_amount", DoubleType(), True),
    StructField("transaction_type_code", StringType(), True),  # <-- Fixed
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_ts", StringType(), True)               # <-- Fixed
])
# Read from Kafka (Updated to localhost for Windows execution)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "banking.transactions.raw") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from the Kafka 'value' column
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write to MinIO Bronze Layer
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://bronze/_checkpoints/streaming_events/") \
    .start("s3a://bronze/transaction_events/")

query.awaitTermination()