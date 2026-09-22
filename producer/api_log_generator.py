import random
import uuid
from datetime import datetime, timezone

# Standard endpoints with typical HTTP methods and payload ranges
API_ENDPOINTS = [
    {"path": "/api/v1/auth/login", "method": "POST", "base_payload": (200, 1024)},
    {"path": "/api/v1/user/profile", "method": "GET", "base_payload": (100, 500)},
    {"path": "/api/v1/products", "method": "GET", "base_payload": (1024, 8192)},
    {"path": "/api/v1/checkout", "method": "POST", "base_payload": (512, 2048)},
    {"path": "/api/v1/reports/export", "method": "GET", "base_payload": (2048, 16384)},
    {"path": "/api/v1/search", "method": "GET", "base_payload": (128, 1024)}
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Python-urllib/3.10",
    "PostmanRuntime/7.32.3"
]

COUNTRIES = ["US", "IN", "DE", "GB", "JP", "BR", "CA", "FR"]

# Static pools of normal user IPs
NORMAL_IP_POOL = [f"192.168.1.{i}" for i in range(1, 101)] + [f"10.0.0.{i}" for i in range(1, 101)]

def generate_normal_log(client_ip=None):
    """Generates a realistic API gateway log event representing legitimate traffic."""
    endpoint_info = random.choice(API_ENDPOINTS)
    ip = client_ip or random.choice(NORMAL_IP_POOL)
    
    # 95% success (200), 4% client error (400/404), 1% server error (500)
    rand_val = random.random()
    if rand_val < 0.95:
        status_code = 200
    elif rand_val < 0.99:
        status_code = random.choice([400, 404])
    else:
        status_code = 500

    min_p, max_p = endpoint_info["base_payload"]
    payload_size = random.randint(min_p, max_p)
    response_time = random.randint(15, 250) if status_code == 200 else random.randint(300, 1200)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "request_id": f"req_{uuid.uuid4().hex[:10]}",
        "client_ip": ip,
        "endpoint": endpoint_info["path"],
        "http_method": endpoint_info["method"],
        "status_code": status_code,
        "response_time_ms": response_time,
        "bytes_sent": payload_size,
        "user_agent": random.choice(USER_AGENTS),
        "country": random.choice(COUNTRIES)
    }
