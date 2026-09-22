# Real-Time API Threat & Anomaly Detection Platform

[![Big Data](https://img.shields.io/badge/BigData-Apache%20Kafka%20%7C%20Spark%20%7C%20Hive-orange.svg)](#)
[![Machine Learning](https://img.shields.io/badge/ML-Isolation%20Forest%20%7C%20Z--Scores-blue.svg)](#)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](#)

A production-grade, distributed streaming security analytics platform engineered to ingest high-velocity API request logs via **Apache Kafka**, perform stateful sliding-window security analytics and hybrid Machine Learning anomaly detection in **Apache Spark Structured Streaming**, persist raw logs and threat flags in **Hadoop HDFS / Apache Hive (Parquet/ORC)**, and visualize live metrics in an **Interactive Web Dashboard**.

---

## 📌 Architecture Diagram

```
┌─────────────────────┐
│ API Log Generator   │
│ 1K-10K events/sec   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Apache Kafka Broker │
│ topic: api-raw-logs │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────┐
│ Spark Structured Streaming Engine       │
│                                         │
│ 1. Parse & Schema Validation            │
│ 2. 10-Min Watermark & 60s/10s Sliding   │
│ 3. Rule & Isolation Forest ML Detection │
└──────────┬──────────────────┬───────────┘
           │                  │
           ▼                  ▼
┌──────────────────┐  ┌──────────────────┐
│ HDFS / Hive      │  │ Kafka Alert      │
│ Parquet & ORC    │  │ topic:           │
│ External Tables  │  │ threat-alerts    │
└──────────┬───────┘  └────────┬─────────┘
           │                   │
           ▼                   ▼
┌──────────────────┐  ┌──────────────────┐
│ Hive SQL         │  │ Interactive Web  │
│ Forensics        │  │ Dashboard        │
└──────────────────┘  └──────────────────┘
```

---

## 🚀 Key Features & Capabilities

- **High-Velocity Ingestion**: Multi-threaded Python generator producing realistic API logs ($1,000\text{--}10,000+$ req/sec) with `client_ip` Kafka key partitioning.
- **Stateful Sliding Window Analytics**: Computes 60-second sliding windows with 10-second slide ($W=60\text{s}, S=10\text{s}$) and 10-minute watermarking for out-of-order logs.
- **Hybrid Threat Detection**:
  - **Rule-Based Detectors**: Instant detection of DDoS floods ($>100$ req/min), HTTP 401 Brute-Force auth attacks ($>10$ failures/min), Endpoint Abuse, HTTP 5xx Error Spikes, and Payload Exfiltration ($>10\text{MB}$).
  - **Machine Learning Detector**: Multi-dimensional **Isolation Forest** scoring over feature vectors (`[total_requests, failed_logins, error_5xx_count, avg_response_time, total_bytes]`).
- **Analytical Data Warehouse**: Persists raw logs to HDFS in compressed **Snappy Parquet** format and enriched threat alerts in **ORC** format for fast Hive SQL post-incident forensics.
- **Interactive Web Dashboard & Control Center**: Real-time FastAPI dashboard with Chart.js metric streaming, live Kafka log feeds, Hive SQL console, and one-click attack triggers.
- **Seamless Handover Log**: Includes [`PROJECT_LOG.md`](file:///c:/Users/amits/Downloads/Real-Time%20API%20Threat%20&%20Anomaly%20Detection%20Platform/PROJECT_LOG.md) for context tracking when switching AI models or developer handovers.

---

## 🛠 Directory Structure

```
Real-Time API Threat & Anomaly Detection Platform/
├── PROJECT_LOG.md                      # Seamless Context Tracker for Model Switching
├── README.md                           # Master Project Documentation & Quickstart
├── docker-compose.yml                  # Complete Local Cluster Stack (Kafka, Spark, Hadoop, Hive)
├── requirements.txt                    # Python Dependencies
├── config/                             # Kafka, Spark, and Threshold Configuration
├── producer/                           # Log Synthesizer & Attack Generator Engine
├── spark/                              # PySpark Structured Streaming & Anomaly Detectors
├── hive/                               # Hive DDL Tables & Forensics Queries
├── dashboard/                          # FastAPI Backend & Web UI Dashboard
├── tests/                              # Automated Unit Test Suite
└── docs/                               # System Architecture, Algorithms & Demo Script
```

---

## 💻 Quickstart Guide

### 1. Start Infrastructure Stack
```bash
docker-compose up -d
```

### 2. Run PySpark Streaming Job
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark/streaming_job.py
```

### 3. Launch Log Producer & Attack Simulator
```bash
# Normal Traffic Flow
python producer/kafka_producer.py --rate 100

# Inject DDoS Attack Scenario
python producer/kafka_producer.py --rate 500 --attack ddos
```

### 4. Launch Web Dashboard
```bash
python dashboard/app.py
# Access dashboard at http://localhost:8000
```

---

## 🧪 Automated Testing
Run the complete unit test suite:
```bash
pytest tests/
```

---

## 💡 Key Technical & Interview Concepts

1. **Kafka Partitioning Strategy**: Why partition by `client_ip`? Kafka guarantees message ordering strictly within a single partition. Hashing by `client_ip` ensures all events from a given IP land on the same partition, preserving sequential accuracy for sliding-window evaluations while enabling horizontal parallelism across Spark executors.
2. **Spark Structured Streaming Watermarking**: Why use `withWatermark("event_timestamp", "10 minutes")`? Watermarking defines how late data can arrive before being dropped. It allows Spark to safely prune old state memory for window aggregations older than 10 minutes, preventing Out-Of-Memory (OOM) crashes under continuous operations.
3. **Dual Storage Strategy (Parquet vs ORC)**: Raw un-aggregated events are stored in **Snappy Parquet** for bulk append throughput, while security alerts are saved in **Hive ORC** format for fast column pruning and predicate pushdowns during SQL forensics.
