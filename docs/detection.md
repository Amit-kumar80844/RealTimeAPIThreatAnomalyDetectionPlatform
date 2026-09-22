# Hybrid Security Anomaly Detection Algorithms

## 1. Hybrid Detection Philosophy
Static threshold rules are fast and effective against known brute force or massive DDoS traffic floods. However, sophisticated attackers evade static limits by slowing down rate limits or altering request patterns. This platform implements a **Hybrid Detection Architecture** combining deterministic rules and multi-dimensional Machine Learning (**Isolation Forest**).

```
                      Spark Micro-batch Window Metrics
                                     │
           ┌─────────────────────────┴─────────────────────────┐
           ▼                                                   ▼
Deterministic Rule Engines                           ML Anomaly Detector
(DDoS, Brute-Force, Abuse, Payloads)                (Isolation Forest & Z-Scores)
           │                                                   │
           └─────────────────────────┬─────────────────────────┘
                                     ▼
                        Hybrid Threat Classification
```

## 2. Deterministic Rule Algorithms

### A. DDoS Flood Detector
Flags IPs exceeding volume threshold within sliding window ($W=60\text{s}$):
$$\text{TotalRequests}_{\text{IP}} > 100 \implies \text{DDoS\_ATTACK} \quad (\text{Severity: CRITICAL})$$

### B. Brute-Force Auth Detector
Flags client IPs accumulating failed authentication responses (HTTP 401):
$$\text{FailedLogins}_{\text{IP}} > 10 \implies \text{BRUTE\_FORCE} \quad (\text{Severity: HIGH})$$

### C. Endpoint Abuse & Error Spikes
Flags IPs with high latency/hit rate on sensitive endpoints or high 5xx ratios:
$$\frac{\text{Error5xxCount}}{\text{TotalRequests}} > 0.20 \implies \text{HTTP\_5XX\_SPIKE} \quad (\text{Severity: MEDIUM})$$

### D. Payload Exfiltration Anomaly
Flags request events sending unusually large byte payloads:
$$\text{BytesSent} > 10,485,760\text{ bytes } (10\text{MB}) \implies \text{PAYLOAD\_EXFILTRATION} \quad (\text{Severity: HIGH})$$

## 3. Machine Learning Anomaly Detector (Isolation Forest & Z-Scores)
An Isolation Forest constructs random binary decision trees over a 5-dimensional feature space:
$$X = [\text{TotalRequests}, \text{FailedLogins}, \text{Error5xxCount}, \text{AvgResponseTime}, \text{TotalBytes}]$$

The anomaly score $s(x, n)$ is derived from the average path length $h(x)$ required to isolate sample $x$:
$$s(x, n) = 2^{-\frac{E(h(x))}{c(n)}}$$
Where $c(n)$ is the average path length of unsuccessful searches in a Binary Search Tree. Path lengths significantly shorter than expected ($s(x, n) > 0.65$) indicate anomalous security events even when no single static threshold is breached.
