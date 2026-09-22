from pyspark.sql.functions import col, when, lit

def detect_ddos(windowed_df, threshold_req_per_min=100):
    """Flags client IPs exceeding request rate threshold within the sliding window."""
    return windowed_df.withColumn(
        "is_ddos",
        when(col("total_requests") > threshold_req_per_min, True).otherwise(False)
    )
