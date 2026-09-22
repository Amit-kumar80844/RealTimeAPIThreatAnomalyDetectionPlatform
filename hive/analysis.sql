USE api_security;

-- Forensics Query 1: Top 5 Attacking IP Addresses by Volume
SELECT client_ip, attack_type, COUNT(*) as attack_instances, SUM(total_requests) as total_volume
FROM threat_alerts
GROUP BY client_ip, attack_type
ORDER BY total_volume DESC
LIMIT 5;

-- Forensics Query 2: High-Severity Brute Force Authentication Analysis
SELECT window_start, window_end, client_ip, failed_logins, anomaly_score
FROM threat_alerts
WHERE attack_type = 'BRUTE_FORCE' AND failed_logins > 10
ORDER BY window_start DESC;

-- Forensics Query 3: Hourly Security Attack Distribution
SELECT HOUR(window_start) as attack_hour, attack_type, COUNT(*) as incident_count
FROM threat_alerts
GROUP BY HOUR(window_start), attack_type
ORDER BY attack_hour ASC;

-- Forensics Query 4: Targeted API Endpoints under DDoS / Endpoint Abuse
SELECT r.endpoint, COUNT(t.client_ip) as attacking_ips, SUM(t.total_requests) as total_hits
FROM threat_alerts t
JOIN raw_api_logs r ON t.client_ip = r.client_ip
WHERE t.attack_type IN ('DDoS_ATTACK', 'ENDPOINT_ABUSE')
GROUP BY r.endpoint
ORDER BY total_hits DESC;
