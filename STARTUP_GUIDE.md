# 🚀 Real-Time API Threat & Anomaly Detection Platform — Complete Startup Guide

> **Quick read**: This guide walks you through every step needed to get the full platform running, from installing dependencies to launching the Spark streaming job and viewing live alerts on the dashboard.

---

## 📋 Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Docker Desktop | ≥ 4.x (Compose V2) | 8 GB RAM + 4 CPU cores minimum |
| Python | 3.10+ | For dashboard server and unit tests |
| Java | 8 (OpenJDK) | Required for `spark-submit` (skip if using Docker only) |
| pip | Latest | `python -m pip install --upgrade pip` |

---

## ⚡ FAST PATH — Dashboard Demo (No Docker Required)

If you only need to demo the dashboard (no real Kafka/Spark):

```powershell
# 1. Create virtual environment
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # macOS/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start dashboard
uvicorn dashboard.app:app --host 0.0.0.0 --port 8000 --reload

# 4. Open browser
# http://localhost:8000
```

The dashboard runs in **full demo/simulation mode** — no external services needed. Use the Live Simulator tab to start traffic, inject attacks, and run Hive SQL forensics.

---

## 🏗️ FULL DEPLOYMENT — All Services via Docker Compose

### Step 1 — Install Python Dependencies

```powershell
# From the project root directory
python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

### Step 2 — Start the Docker Infrastructure Stack

```powershell
# Pull images and start all 8+ services in background
# First run downloads ~2 GB of images — takes 3-5 minutes
docker compose up -d

# Verify all containers are healthy
docker compose ps
```

**Expected services after startup:**

| Container | Purpose | Port |
|---|---|---|
| `zookeeper` | Kafka coordination | 2181 |
| `kafka` | Message broker | 9092 (ext) / 29092 (int) |
| `namenode` | HDFS NameNode | 9870 (UI), 9000 |
| `datanode` | HDFS DataNode | 9864 |
| `spark-master` | Spark Master | 8080 (UI), 7077 |
| `spark-worker` | Spark Worker | 8081 |
| `hive-metastore` | Hive metadata store | internal |
| `hive-server` | HiveServer2 | 10000 |
| `threat-dashboard` | FastAPI Dashboard | 8000 |

### Step 3 — Initialize Hive Tables (First Time Only)

Wait ~30 seconds for Hive to be fully ready, then:

```powershell
# Copy DDL into container and execute via Beeline CLI
docker cp hive/create_tables.sql hive-server:/tmp/

docker exec -it hive-server beeline `
    -u "jdbc:hive2://localhost:10000" `
    -f /tmp/create_tables.sql
```

This creates:
- `api_security.raw_api_logs` — Parquet, partitioned by `dt`
- `api_security.threat_alerts` — ORC, partitioned by `dt`
- `api_security.dead_letter_events` — TEXTFILE for malformed events

### Step 4 — Submit the PySpark Streaming Job

```powershell
# Option A: Submit to local Spark installation
spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 `
    spark/streaming_job.py localhost:9092

# Option B: Submit inside the Docker Spark Master container
docker exec spark-master spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 `
    /app/spark/streaming_job.py kafka:29092
```

The job will print detected security alerts to the console and write them to HDFS/Hive.

### Step 5 — Start the Kafka Log Producer

```powershell
# Normal traffic — 200 events/sec
python -m producer.kafka_producer --rate 200

# DDoS attack — 500 events/sec for 30 seconds
python -m producer.kafka_producer --rate 500 --attack ddos --duration 30

# Brute-force attack
python -m producer.kafka_producer --rate 300 --attack bruteforce

# Endpoint abuse
python -m producer.kafka_producer --rate 200 --attack abuse

# Distributed botnet (random IPs, 185.x.x.x range)
python -m producer.kafka_producer --rate 1000 --attack botnet
```

**Attack options:** `ddos` | `bruteforce` | `abuse` | `errors` | `payload` | `botnet`

---

## 🧪 Running Unit Tests

```powershell
# Run full test suite (8 tests, ~2 seconds, no Kafka/Spark needed)
python -m pytest tests/ -v

# Or with unittest
python -m unittest discover tests -v
```

**Expected output:**
```
test_generate_normal_log_structure ........... ok
test_attack_simulator_ddos .................. ok
test_attack_simulator_brute_force ........... ok
test_attack_simulator_payload_exfiltration .. ok
test_calculate_z_score ...................... ok
test_calculate_stats ........................ ok
test_api_log_schema_fields .................. ok
test_alert_schema_fields .................... ok

Ran 8 tests in ~1.8s   OK
```

---

## 🌐 Web Interface URLs

| Interface | URL | Notes |
|---|---|---|
| **Threat Dashboard** | http://localhost:8000 | Main dashboard — always available |
| **Spark Master UI** | http://localhost:8080 | Job monitoring, executor status |
| **Spark Worker UI** | http://localhost:8081 | Worker-level metrics |
| **HDFS NameNode UI** | http://localhost:9870 | File system browser |
| **Kafka Broker** | `localhost:9092` | External access from host |

---

## 🔍 Hive SQL Forensics Queries

Connect via Beeline or use the built-in Forensics Console in the dashboard:

```sql
-- 1. Top attacking IPs
SELECT client_ip, attack_type, COUNT(*) as alert_count, SUM(total_requests) as total_volume
FROM threat_alerts
GROUP BY client_ip, attack_type
ORDER BY total_volume DESC;

-- 2. Brute-force deep dive
SELECT window_start, client_ip, failed_logins, anomaly_score
FROM threat_alerts
WHERE attack_type = 'BRUTE_FORCE'
ORDER BY window_start DESC;

-- 3. Hourly attack distribution
SELECT HOUR(window_start) as hour, attack_type, COUNT(*) as incidents
FROM threat_alerts
GROUP BY HOUR(window_start), attack_type
ORDER BY hour;

-- 4. High-severity alerts today
SELECT * FROM threat_alerts
WHERE severity = 'CRITICAL' AND dt = CURRENT_DATE;
```

---

## 🛑 Teardown

```powershell
# Stop containers (keep data volumes)
docker compose down

# Stop and destroy all volumes (complete reset)
docker compose down -v
```

---

## 📁 Project File Map

```
Real-Time API Threat & Anomaly Detection Platform/
├── Dockerfile                         ← Dashboard container build spec
├── docker-compose.yml                 ← Full 8-service infrastructure stack
├── requirements.txt                   ← Python dependencies
├── STARTUP_GUIDE.md                   ← This file
├── README.md                          ← Full project documentation
├── PROJECT_LOG.md                     ← Handover context tracker
│
├── config/
│   ├── anomaly_thresholds.json        ← Detection threshold configuration
│   ├── kafka.yaml                     ← Kafka topic settings
│   ├── spark.conf                     ← Spark streaming configuration
│   └── hadoop.env                     ← HDFS environment variables
│
├── producer/
│   ├── __init__.py
│   ├── api_log_generator.py           ← Normal traffic event generator
│   ├── attack_simulator.py            ← 5 attack scenario generators
│   └── kafka_producer.py              ← Multi-threaded Kafka publisher
│
├── spark/
│   ├── __init__.py
│   ├── streaming_job.py               ← PySpark Structured Streaming driver
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── api_schema.py              ← StructType schema + stream validator
│   ├── detection/
│   │   ├── __init__.py
│   │   ├── ddos_detector.py           ← DDoS flood rule detector
│   │   ├── brute_force_detector.py    ← Auth failure rate detector
│   │   ├── endpoint_abuse_detector.py ← Endpoint abuse + error spike + payload
│   │   └── ml_anomaly_detector.py     ← Isolation Forest + Z-Score ML engine
│   └── utils/
│       ├── __init__.py
│       └── metrics.py                 ← Z-score + statistical helpers
│
├── hive/
│   ├── create_tables.sql              ← Hive DDL (Parquet + ORC tables)
│   ├── analysis.sql                   ← Forensic SQL queries
│   └── reports.sql                    ← Executive threat summary SQL
│
├── dashboard/
│   ├── app.py                         ← FastAPI backend + SSE streaming
│   └── templates/
│       └── index.html                 ← Full interactive web dashboard
│
├── tests/
│   ├── __init__.py
│   ├── test_producer.py               ← 4 producer unit tests
│   ├── test_detection.py              ← 2 detection metric tests
│   └── test_schema.py                 ← 2 schema validation tests
│
└── docs/
    ├── architecture.md                ← Architecture breakdown
    ├── detection.md                   ← Detection algorithm documentation
    └── demo.md                        ← 10-minute demo script
```

---

*CSE412 Applied Big Data — Real-Time API Threat & Anomaly Detection Platform*
