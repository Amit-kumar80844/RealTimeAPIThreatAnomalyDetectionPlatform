from pyspark.sql.functions import col, when

def detect_endpoint_abuse(windowed_df, threshold_hits=50, threshold_5xx_rate=0.20, threshold_payload=10485760):
    """Flags endpoint abuse, server error rate spikes, and large data exfiltration payloads."""
    return windowed_df \
        .withColumn(
            "is_endpoint_abuse",
            when((col("total_requests") > threshold_hits) & (col("avg_response_time") > 1000), True).otherwise(False)
        ) \
        .withColumn(
            "is_error_spike",
            when((col("total_requests") > 10) & ((col("error_5xx_count") / col("total_requests")) > threshold_5xx_rate), True).otherwise(False)
        ) \
        .withColumn(
            "is_large_payload",
            when(col("total_bytes") > threshold_payload, True).otherwise(False)
        )
