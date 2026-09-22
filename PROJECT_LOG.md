# Real-Time API Threat & Anomaly Detection Platform - Handover & Context Log

> **Purpose**: This log tracks the ongoing state, architectural decisions, file maps, and progress of the **Real-Time API Threat & Anomaly Detection Platform**. If you switch AI models or context windows, read this file first to instantly understand what has been completed, how components interface, and what steps remain.

---

## 📌 Project Overview
- **Title**: Real-Time API Threat & Anomaly Detection Platform
- **Goal**: Ingest 1K-10K+ API gateway logs/sec via Apache Kafka, process stateful 60s sliding windows with 10s slide in Apache Spark Structured Streaming, apply rule-based + ML (Isolation Forest) anomaly detection, persist raw/alert data in Hadoop HDFS / Hive (Parquet/ORC), and provide an interactive Web Dashboard & SQL forensics console.
- **Tech Stack**: Python 3.10+, Apache Kafka, Apache Spark Structured Streaming, PySpark, scikit-learn, Apache Hive, Hadoop HDFS, FastAPI, TailwindCSS, Chart.js, Docker Compose, Pytest.

---

## 🗂 File Directory Map

```
c:\Users\amits\Downloads\Real-Time API Threat & Anomaly Detection Platform\
├── PROJECT_LOG.md                      <-- (THIS FILE) Handover context tracker
├── README.md                           # Master Documentation & Deployment Guide
├── docker-compose.yml                  # Infrastructure Stack (Kafka, Spark, Hadoop, Hive, Dashboard)
├── requirements.txt                    # Project Python dependencies
├── config/
│   ├── hadoop.env                      # HDFS NameNode / DataNode environment vars
│   ├── kafka.yaml                      # Kafka topics & partition settings
│   ├── spark.conf                      # Spark streaming configurations
│   └── anomaly_thresholds.json         # Security alert thresholds
├── producer/
│   ├── api_log_generator.py            # Synthetic log event generator
│   ├── attack_simulator.py             # Targeted attack scenario synthesizer
│   └── kafka_producer.py               # Kafka publisher with client_ip partitioning
├── spark/
│   ├── streaming_job.py                # PySpark Structured Streaming Driver
│   ├── schemas/api_schema.py           # StructType JSON schema & validation
│   ├── detection/
│   │   ├── ddos_detector.py            # DDoS traffic flood detection
│   │   ├── brute_force_detector.py     # Brute-force auth attempt detection
│   │   ├── endpoint_abuse_detector.py  # Resource/endpoint exhaustion detection
│   │   └── ml_anomaly_detector.py      # Isolation Forest & Z-Score anomaly engine
│   └── utils/metrics.py                # Statistical helper utilities
├── hive/
│   ├── create_tables.sql               # Hive DDL external tables (Parquet/ORC)
│   ├── analysis.sql                    # Forensics SQL queries
│   └── reports.sql                     # Executive threat summary queries
├── dashboard/
│   ├── app.py                          # FastAPI backend server with streaming & simulation APIs
│   └── templates/index.html            # Web Dashboard UI with charts, log feeds & controls
├── tests/
│   ├── test_producer.py                # Producer unit tests
│   ├── test_detection.py               # Detection engine unit tests
│   └── test_schema.py                  # Schema validation tests
└── docs/
    ├── architecture.md                 # Architecture breakdown
    ├── detection.md                    # Detection algorithm documentation
    └── demo.md                         # 10-Minute Video script & demo walkthrough
```

---

## 🚀 Execution & Implementation Status

| Stage / Component | Status | Description |
| :--- | :---: | :--- |
| **Project Plan & Context Tracker** | ✅ Completed | `PROJECT_LOG.md` & `implementation_plan.md` created and maintained. |
| **Docker Compose Stack** | ✅ Completed | `docker-compose.yml` & `config/hadoop.env` configured for cluster. |
| **Configuration Files** | ✅ Completed | `kafka.yaml`, `spark.conf`, `anomaly_thresholds.json` defined. |
| **Log Producer & Attack Simulator** | ✅ Completed | `api_log_generator.py`, `attack_simulator.py`, `kafka_producer.py` built. |
| **Spark Structured Streaming Engine** | ✅ Completed | `streaming_job.py`, `api_schema.py`, rule detectors, `ml_anomaly_detector.py`. |
| **Hive Schema & SQL Forensics** | ✅ Completed | DDL (`create_tables.sql`), `analysis.sql`, `reports.sql` created. |
| **Interactive Web Dashboard** | ✅ Completed | FastAPI backend (`app.py`) & Tailwind/Chart.js frontend (`index.html`). |
| **Unit Tests & Test Suite** | ✅ Completed | 8/8 tests passing (`python -m unittest discover tests`). |
| **Documentation & Video Script** | ✅ Completed | `README.md`, `architecture.md`, `detection.md`, `demo.md`. |

---

## 💡 Key Architectural Decisions
1. **IP-Based Kafka Partitioning**: Kafka topic `api-raw-logs` uses `client_ip` as the partitioning key to guarantee per-client event ordering within a single partition while achieving parallel consumption across Spark executors.
2. **Hybrid Anomaly Detection**:
   - **Deterministic Rules**: Instantly flag known attack thresholds (e.g. $>100$ req/min $\rightarrow$ DDoS, $>10$ HTTP 401/min $\rightarrow$ Brute Force).
   - **Machine Learning (Isolation Forest & Z-Scores)**: Detects subtle multi-dimensional anomalies (e.g. combined moderate increase in request frequency + high payload size + unusual endpoint sequence).
3. **Dual Data Persistence Strategy**:
   - **Raw Events**: Appended to HDFS in compressed Snappy Parquet format (`/user/hive/warehouse/api_security.db/raw_api_logs`) partitioned by date (`dt`).
   - **Alert Records**: Saved to Hive ORC table (`/user/hive/warehouse/threat_alerts`) partitioned by date for ultra-fast column-pruned analytical queries.

---

## 📋 Verification Results
- Executed `python -m unittest discover tests`: All 8 unit tests passed cleanly (`OK`).
