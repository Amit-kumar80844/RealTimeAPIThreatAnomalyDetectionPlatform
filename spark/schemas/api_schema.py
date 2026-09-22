try:
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, TimestampType, LongType
    )
    from pyspark.sql.functions import from_json, col, when
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    StructType = StructField = StringType = IntegerType = TimestampType = LongType = None

def get_api_log_schema():
    """Returns the explicit PySpark StructType schema for incoming API log events."""
    if not HAS_PYSPARK:
        return [
            "timestamp", "request_id", "client_ip", "endpoint",
            "http_method", "status_code", "response_time_ms",
            "bytes_sent", "user_agent", "country"
        ]
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("client_ip", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("http_method", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("bytes_sent", LongType(), True),
        StructField("user_agent", StringType(), True),
        StructField("country", StringType(), True)
    ])

def get_alert_schema():
    """Returns the explicit PySpark StructType schema for persistent security alerts."""
    if not HAS_PYSPARK:
        return [
            "window_start", "window_end", "client_ip", "total_requests",
            "failed_logins", "error_5xx_count", "avg_response_time",
            "total_bytes", "attack_type", "severity", "anomaly_score"
        ]
    return StructType([
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("client_ip", StringType(), True),
        StructField("total_requests", LongType(), True),
        StructField("failed_logins", LongType(), True),
        StructField("error_5xx_count", LongType(), True),
        StructField("avg_response_time", IntegerType(), True),
        StructField("total_bytes", LongType(), True),
        StructField("attack_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("anomaly_score", StringType(), True)
    ])

def parse_and_validate_stream(raw_stream_df):
    """Parses raw JSON strings from Kafka, applies schema, and splits valid vs malformed dead-letter events."""
    if not HAS_PYSPARK:
        raise RuntimeError("PySpark is required to execute stream parsing.")

    schema = get_api_log_schema()
    parsed_df = raw_stream_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", col("timestamp").cast(TimestampType()))

    validated_df = parsed_df.withColumn(
        "is_valid",
        when(
            col("client_ip").isNotNull() &
            col("endpoint").isNotNull() &
            col("event_timestamp").isNotNull(),
            True
        ).otherwise(False)
    )

    valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid")
    dead_letter_df = validated_df.filter(col("is_valid") == False)

    return valid_df, dead_letter_df
