USE api_security;

-- Executive Report 1: Daily Threat Breakdown by Severity
SELECT dt, severity, COUNT(*) as total_alerts, SUM(total_requests) as impacted_requests
FROM threat_alerts
GROUP BY dt, severity
ORDER BY dt DESC, total_alerts DESC;

-- Executive Report 2: Geolocation Threat Matrix
SELECT r.country, t.attack_type, COUNT(DISTINCT t.client_ip) as unique_attackers
FROM threat_alerts t
JOIN raw_api_logs r ON t.client_ip = r.client_ip
GROUP BY r.country, t.attack_type
ORDER BY unique_attackers DESC;
