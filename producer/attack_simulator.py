import random
import uuid
from datetime import datetime, timezone

ATTACKER_IPS = ["192.168.1.250", "10.0.0.99", "172.16.0.42", "198.51.100.7"]

class AttackSimulator:
    """Generates attack payloads for DDoS, Brute-Force, Endpoint Abuse, Error Spikes & Large Payloads."""

    @staticmethod
    def generate_ddos_event(target_ip="192.168.1.250"):
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": f"req_ddos_{uuid.uuid4().hex[:8]}",
            "client_ip": target_ip,
            "endpoint": "/api/v1/products",
            "http_method": "GET",
            "status_code": 200,
            "response_time_ms": random.randint(30, 80),
            "bytes_sent": random.randint(500, 1500),
            "user_agent": "Golang-HTTP-Client/1.1",
            "country": "RU"
        }

    @staticmethod
    def generate_brute_force_event(target_ip="10.0.0.99"):
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": f"req_bf_{uuid.uuid4().hex[:8]}",
            "client_ip": target_ip,
            "endpoint": "/api/v1/auth/login",
            "http_method": "POST",
            "status_code": 401,  # Authentication Failure
            "response_time_ms": random.randint(10, 40),
            "bytes_sent": 256,
            "user_agent": "Hydra-Bot/7.1",
            "country": "CN"
        }

    @staticmethod
    def generate_endpoint_abuse_event(target_ip="172.16.0.42"):
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": f"req_abuse_{uuid.uuid4().hex[:8]}",
            "client_ip": target_ip,
            "endpoint": "/api/v1/reports/export",
            "http_method": "GET",
            "status_code": 200,
            "response_time_ms": random.randint(1500, 4000),  # Heavy DB processing
            "bytes_sent": random.randint(1048576, 5242880),
            "user_agent": "Scrapy/2.9.0",
            "country": "IR"
        }

    @staticmethod
    def generate_error_spike_event(target_ip="198.51.100.7"):
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": f"req_err_{uuid.uuid4().hex[:8]}",
            "client_ip": target_ip,
            "endpoint": "/api/v1/checkout",
            "http_method": "POST",
            "status_code": 503,  # Service Unavailable / Crash
            "response_time_ms": random.randint(500, 2500),
            "bytes_sent": 128,
            "user_agent": "Mozilla/5.0",
            "country": "US"
        }

    @staticmethod
    def generate_large_payload_event(target_ip="192.168.1.250"):
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": f"req_exfil_{uuid.uuid4().hex[:8]}",
            "client_ip": target_ip,
            "endpoint": "/api/v1/user/profile",
            "http_method": "POST",
            "status_code": 200,
            "response_time_ms": random.randint(800, 3000),
            "bytes_sent": random.randint(12500000, 25000000),  # 12.5MB - 25MB Payload anomaly
            "user_agent": "CustomExfilScript/1.0",
            "country": "KP"
        }

    @staticmethod
    def generate_distributed_ddos_event():
        bot_ip = f"185.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": f"req_botnet_{uuid.uuid4().hex[:8]}",
            "client_ip": bot_ip,
            "endpoint": "/api/v1/auth/login",
            "http_method": "POST",
            "status_code": 401,
            "response_time_ms": random.randint(20, 60),
            "bytes_sent": 320,
            "user_agent": "MiraiBotnet/2.0",
            "country": random.choice(["RU", "CN", "BR", "UA"])
        }
