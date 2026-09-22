-- Initialize Hive Security Database
CREATE DATABASE IF NOT EXISTS api_security;
USE api_security;

-- 1. Raw API Gateway Logs Table (Partitioned by Date)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_api_logs (
    request_id STRING,
    client_ip STRING,
    endpoint STRING,
    http_method STRING,
    status_code INT,
    response_time_ms INT,
    bytes_sent BIGINT,
    user_agent STRING,
    country STRING,
    event_timestamp TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/api_security.db/raw_api_logs';

-- 2. Persistent Threat Alerts Table (ORC Format for Fast Analytical Aggregations)
CREATE EXTERNAL TABLE IF NOT EXISTS threat_alerts (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    client_ip STRING,
    total_requests BIGINT,
    failed_logins BIGINT,
    error_5xx_count BIGINT,
    avg_response_time INT,
    total_bytes BIGINT,
    attack_type STRING,
    severity STRING,
    anomaly_score DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS ORC
LOCATION '/user/hive/warehouse/api_security.db/threat_alerts';

-- 3. Dead-Letter Malformed Events Queue Table
CREATE EXTERNAL TABLE IF NOT EXISTS dead_letter_events (
    raw_payload STRING,
    ingestion_time TIMESTAMP,
    error_reason STRING
)
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/api_security.db/dead_letter_events';
