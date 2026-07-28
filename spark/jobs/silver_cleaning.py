from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_json, struct

# ... [Assume Spark is initialized and bronze_stream is loaded as before] ...

# ---------------------------------------------------------
# 1. Splitting the Stream (Good vs. Bad Data)
# ---------------------------------------------------------
# BAD DATA: Missing critical IDs (Send to DLQ)
malformed_stream = bronze_stream.filter(col("transaction_id").isNull() | col("account_id").isNull())

# GOOD DATA: Valid records (Send to Validated)
validated_stream = bronze_stream.filter(col("transaction_id").isNotNull() & col("account_id").isNotNull())

# FRAUD DATA: Valid records that trigger a simple risk rule (e.g., amount > 10,000)
fraud_stream = validated_stream.filter(col("amount") > 10000)

# ---------------------------------------------------------
# 2. Writing to Kafka: The DLQ Topic
# ---------------------------------------------------------
print("Routing malformed records to DLQ...")
dlq_query = malformed_stream \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "banking.transactions.dlq") \
    .option("checkpointLocation", "s3a://silver/_checkpoints/kafka_dlq/") \
    .start()

# ---------------------------------------------------------
# 3. Writing to Kafka: The Validated Topic
# ---------------------------------------------------------
print("Routing clean records to Validated topic...")
validated_query = validated_stream \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "banking.transactions.validated") \
    .option("checkpointLocation", "s3a://silver/_checkpoints/kafka_validated/") \
    .start()

# ---------------------------------------------------------
# 4. Writing to Kafka: The Fraud Alerts Topic
# ---------------------------------------------------------
print("Routing suspicious records to Fraud Alerts topic...")
fraud_query = fraud_stream \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "banking.fraud.alerts") \
    .option("checkpointLocation", "s3a://silver/_checkpoints/kafka_fraud/") \
    .start()

# Wait for all streams to process
spark.streams.awaitAnyTermination()