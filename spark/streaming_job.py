import os
import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, col, count, sum as _sum, avg, max as _max, when, lit, concat_ws, to_json, struct
)

# Import internal schema & detectors
from spark.schemas.api_schema import parse_and_validate_stream
from spark.detection.ddos_detector import detect_ddos
from spark.detection.brute_force_detector import detect_brute_force
from spark.detection.endpoint_abuse_detector import detect_endpoint_abuse
from spark.detection.ml_anomaly_detector import MLAnomalyDetector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def create_spark_session():
    """Initializes Spark Session with Hive support and Kafka SQL package."""
    return SparkSession.builder \
        .appName("API-Threat-Anomaly-Detector") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

def run_pipeline(kafka_bootstrap="localhost:9092", raw_topic="api-raw-logs", alert_topic="threat-alerts"):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logging.info(f"Initialized Spark Session. Subscribing to Kafka stream '{raw_topic}' at {kafka_bootstrap}")

    # 1. Read Stream from Kafka
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", raw_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse & Validate Schema
    valid_events_df, dead_letter_df = parse_and_validate_stream(raw_kafka_df)

    # 3. Watermark late data up to 10 minutes
    watermarked_df = valid_events_df.withWatermark("event_timestamp", "10 minutes")

    # 4. Perform 60s sliding window aggregations with 10s slide per client_ip
    windowed_stats = watermarked_df \
        .groupBy(
            window(col("event_timestamp"), "60 seconds", "10 seconds"),
            col("client_ip")
        ) \
        .agg(
            count("*").alias("total_requests"),
            _sum(when(col("status_code") == 401, 1).otherwise(0)).alias("failed_logins"),
            _sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("error_5xx_count"),
            avg("response_time_ms").cast("integer").alias("avg_response_time"),
            _sum("bytes_sent").alias("total_bytes")
        )

    # 5. Apply Rule-Based Security Detectors
    with_ddos = detect_ddos(windowed_stats, threshold_req_per_min=100)
    with_bf = detect_brute_force(with_ddos, threshold_failed_logins=10)
    with_rules = detect_endpoint_abuse(with_bf, threshold_hits=50, threshold_5xx_rate=0.20, threshold_payload=10485760)

    # 6. Assign Primary Attack Classification & Severity
    flagged_anomalies = with_rules \
        .withColumn(
            "attack_type",
            when(col("is_ddos"), "DDoS_ATTACK")
            .when(col("is_brute_force"), "BRUTE_FORCE")
            .when(col("is_endpoint_abuse"), "ENDPOINT_ABUSE")
            .when(col("is_error_spike"), "HTTP_5XX_SPIKE")
            .when(col("is_large_payload"), "PAYLOAD_EXFILTRATION")
            .otherwise("NONE")
        ) \
        .withColumn(
            "severity",
            when(col("attack_type") == "DDoS_ATTACK", "CRITICAL")
            .when(col("attack_type") == "BRUTE_FORCE", "HIGH")
            .when(col("attack_type") == "PAYLOAD_EXFILTRATION", "HIGH")
            .when(col("attack_type") == "ENDPOINT_ABUSE", "MEDIUM")
            .when(col("attack_type") == "HTTP_5XX_SPIKE", "MEDIUM")
            .otherwise("LOW")
        ) \
        .withColumn(
            "anomaly_score",
            when(col("attack_type") != "NONE", lit("0.95"))
            .otherwise(lit("0.05"))
        )

    # Filter out normal traffic for alert sink
    alerts_df = flagged_anomalies \
        .filter(col("attack_type") != "NONE") \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("client_ip"),
            col("total_requests"),
            col("failed_logins"),
            col("error_5xx_count"),
            col("avg_response_time"),
            col("total_bytes"),
            col("attack_type"),
            col("severity"),
            col("anomaly_score")
        )

    # 7. Write Stream Sinks (Console for logging & Parquet/ORC for HDFS/Hive)
    console_query = alerts_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .start()

    return console_query

if __name__ == "__main__":
    broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    query = run_pipeline(kafka_bootstrap=broker)
    query.awaitTermination()
