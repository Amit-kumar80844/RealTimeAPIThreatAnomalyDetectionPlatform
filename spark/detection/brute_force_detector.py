from pyspark.sql.functions import col, when

def detect_brute_force(windowed_df, threshold_failed_logins=10):
    """Flags client IPs exceeding authentication failure threshold (HTTP 401) within window."""
    return windowed_df.withColumn(
        "is_brute_force",
        when(col("failed_logins") > threshold_failed_logins, True).otherwise(False)
    )
