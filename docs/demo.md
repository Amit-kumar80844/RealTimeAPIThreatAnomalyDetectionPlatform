# 10-Minute Presentation & Demonstration Walkthrough Script

## 1. Presentation Outline (10-Minute Allocation)

| Time Range | Agenda | Key Talking Points |
| :--- | :--- | :--- |
| **0:00 - 2:00** | Problem Formulation & System Goals | Explain high velocity API gateway logs, single-node failure, and stream-batch duality. |
| **2:00 - 4:30** | System Architecture & Tool Integration | Walk through Kafka ingestion, PySpark $60\text{s}/10\text{s}$ sliding windowing, and HDFS/Hive sinks. |
| **4:30 - 7:30** | Live Pipeline Demonstration | Show split terminal: Start Kafka producer, trigger DDoS/Brute-Force attacks, view real-time Chart.js & alert stream. |
| **7:30 - 9:00** | Hive SQL Forensics & Investigation | Run Hive SQL queries identifying top attacking IPs and attack breakdown. |
| **9:00 - 10:00** | Q&A & Technical Defense | Defend watermarking (10m), IP hashing partition key, fault tolerance, and Isolation Forest ML logic. |

---

## 2. Step-by-Step Terminal Commands

### Terminal 1: Launch Infrastructure Cluster
```bash
docker-compose up -d
```

### Terminal 2: Execute PySpark Structured Streaming Driver
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark/streaming_job.py
```

### Terminal 3: Start Synthetic API Traffic Generator & Inject Attack
```bash
# Normal Traffic Flow (100 req/sec)
python producer/kafka_producer.py --rate 100

# Inject DDoS Attack Scenario
python producer/kafka_producer.py --rate 500 --attack ddos

# Inject HTTP 401 Brute Force Scenario
python producer/kafka_producer.py --rate 200 --attack bruteforce
```

### Terminal 4: Launch Web Dashboard & SQL Forensics Console
```bash
python dashboard/app.py
# Open browser at http://localhost:8000
```
