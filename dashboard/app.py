"""
dashboard/app.py
================
FastAPI backend for the Real-Time API Threat & Anomaly Detection Dashboard.

Features:
  - Server-Sent Events (SSE) endpoint /api/stream  → pushes live log/alert deltas to browser
  - REST endpoints for attack injection, state inspection, and simulated Hive queries
  - In-memory simulation engine (no Kafka required for demo mode)
  - Graceful fallback if producer modules are unavailable
"""

import os
import sys
import json
import asyncio
import random
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ── Add project root to sys.path so producer modules are importable ────────────
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from producer.api_log_generator import generate_normal_log, ATTACKER_IPS  # type: ignore
    ATTACKER_IPS = ["192.168.1.250", "10.0.0.99", "172.16.0.42", "198.51.100.7"]
except ImportError:
    ATTACKER_IPS = ["192.168.1.250", "10.0.0.99", "172.16.0.42", "198.51.100.7"]
    def generate_normal_log(client_ip=None):
        import uuid
        endpoints = ["/api/v1/products", "/api/v1/auth/login", "/api/v1/search",
                     "/api/v1/user/profile", "/api/v1/checkout", "/api/v1/reports/export"]
        ip_pool = [f"192.168.1.{i}" for i in range(1, 51)] + [f"10.0.0.{i}" for i in range(1, 51)]
        ip = client_ip or random.choice(ip_pool)
        status = random.choices([200, 400, 404, 500], weights=[92, 3, 4, 1])[0]
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": f"req_{uuid.uuid4().hex[:10]}",
            "client_ip": ip,
            "endpoint": random.choice(endpoints),
            "http_method": random.choice(["GET", "POST"]),
            "status_code": status,
            "response_time_ms": random.randint(10, 300),
            "bytes_sent": random.randint(200, 16000),
            "user_agent": "Mozilla/5.0",
            "country": random.choice(["US", "IN", "DE", "JP", "BR"])
        }

try:
    from producer.attack_simulator import AttackSimulator  # type: ignore
except ImportError:
    class AttackSimulator:  # type: ignore
        @staticmethod
        def generate_ddos_event(target_ip="192.168.1.250"):
            return {"client_ip": target_ip, "endpoint": "/api/v1/products",
                    "status_code": 200, "response_time_ms": 45, "bytes_sent": 800,
                    "timestamp": datetime.now(timezone.utc).isoformat()}
        @staticmethod
        def generate_brute_force_event(target_ip="10.0.0.99"):
            return {"client_ip": target_ip, "endpoint": "/api/v1/auth/login",
                    "status_code": 401, "response_time_ms": 25, "bytes_sent": 256,
                    "timestamp": datetime.now(timezone.utc).isoformat()}
        @staticmethod
        def generate_endpoint_abuse_event(target_ip="172.16.0.42"):
            return {"client_ip": target_ip, "endpoint": "/api/v1/reports/export",
                    "status_code": 200, "response_time_ms": 2800, "bytes_sent": 2097152,
                    "timestamp": datetime.now(timezone.utc).isoformat()}
        @staticmethod
        def generate_error_spike_event(target_ip="198.51.100.7"):
            return {"client_ip": target_ip, "endpoint": "/api/v1/checkout",
                    "status_code": 503, "response_time_ms": 1200, "bytes_sent": 128,
                    "timestamp": datetime.now(timezone.utc).isoformat()}
        @staticmethod
        def generate_large_payload_event(target_ip="192.168.1.250"):
            return {"client_ip": target_ip, "endpoint": "/api/v1/user/profile",
                    "status_code": 200, "response_time_ms": 1900, "bytes_sent": 18400000,
                    "timestamp": datetime.now(timezone.utc).isoformat()}


# ── FastAPI Application ───────────────────────────────────────────────────────
app = FastAPI(
    title="Real-Time API Threat Detection Dashboard",
    description="CSE412 Applied Big Data — Kafka + Spark Streaming + Hive Security Platform",
    version="1.0.0"
)

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# ── Global Simulation State ───────────────────────────────────────────────────
STATE = {
    "is_streaming": False,
    "events_streamed": 0,
    "alerts_triggered": 0,
    "active_attack": None,
    "attack_expires_at": 0.0,
    "recent_logs": [],
    "recent_alerts": [],
    "window_metrics": {
        "labels": [],
        "req_counts": []
    },
    "severity_counts": {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
}

# SSE subscriber queues (one asyncio.Queue per open /api/stream connection)
_sse_subscribers: list[asyncio.Queue] = []

HIVE_SIMULATED_ROWS = [
    {"client_ip": "192.168.1.250", "attack_type": "DDoS_ATTACK",        "alert_count": 18, "total_volume": "2,450 reqs", "severity": "CRITICAL", "first_seen": "2026-09-17 11:32:14"},
    {"client_ip": "10.0.0.99",     "attack_type": "BRUTE_FORCE",        "alert_count": 9,  "total_volume": "410 reqs",   "severity": "HIGH",     "first_seen": "2026-09-17 11:45:02"},
    {"client_ip": "172.16.0.42",   "attack_type": "ENDPOINT_ABUSE",     "alert_count": 5,  "total_volume": "1,120 reqs", "severity": "MEDIUM",   "first_seen": "2026-09-17 12:01:38"},
    {"client_ip": "198.51.100.7",  "attack_type": "HTTP_5XX_SPIKE",     "alert_count": 4,  "total_volume": "680 reqs",   "severity": "MEDIUM",   "first_seen": "2026-09-17 12:15:50"},
    {"client_ip": "185.44.201.12", "attack_type": "PAYLOAD_EXFILTRATION","alert_count": 2, "total_volume": "38 MB exfil","severity": "HIGH",     "first_seen": "2026-09-17 12:28:07"}
]


def _push_sse_event(event_type: str, data: dict):
    """Broadcast an SSE event to all active subscribers."""
    payload = json.dumps({"type": event_type, "data": data})
    dead = []
    for q in _sse_subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _sse_subscribers.remove(q)


# ── Background Simulation Loop ────────────────────────────────────────────────
async def _simulation_loop():
    """Generates synthetic API log events and pushes them via SSE every 600 ms."""
    import time
    while STATE["is_streaming"]:
        now = time.time()
        # Determine if an attack injection is still active
        if STATE["attack_expires_at"] and now > STATE["attack_expires_at"]:
            STATE["active_attack"] = None
            STATE["attack_expires_at"] = 0.0

        attack = STATE["active_attack"]
        batch = max(1, random.randint(6, 14))
        STATE["events_streamed"] += batch

        # Pick log type based on attack scenario
        if attack == "ddos":
            log = AttackSimulator.generate_ddos_event()
            req_rate = random.randint(118, 148)
        elif attack == "bruteforce":
            log = AttackSimulator.generate_brute_force_event()
            req_rate = random.randint(44, 60)
        elif attack == "abuse":
            log = AttackSimulator.generate_endpoint_abuse_event()
            req_rate = random.randint(52, 72)
        elif attack == "errors":
            log = AttackSimulator.generate_error_spike_event()
            req_rate = random.randint(30, 55)
        elif attack == "payload":
            log = AttackSimulator.generate_large_payload_event()
            req_rate = random.randint(8, 20)
        else:
            log = generate_normal_log()
            req_rate = random.randint(8, 28)

        # Keep recent logs in memory (last 30)
        log_entry = {
            "ip": log.get("client_ip", "?"),
            "endpoint": log.get("endpoint", "/"),
            "status": log.get("status_code", 200),
            "resp_ms": log.get("response_time_ms", 0),
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S")
        }
        STATE["recent_logs"] = [log_entry] + STATE["recent_logs"][:29]

        # Window metric history (last 12 points)
        ts_label = datetime.now(timezone.utc).strftime("%H:%M:%S")
        STATE["window_metrics"]["labels"] = (STATE["window_metrics"]["labels"] + [ts_label])[-12:]
        STATE["window_metrics"]["req_counts"] = (STATE["window_metrics"]["req_counts"] + [req_rate])[-12:]

        # Automatic anomaly detection for SSE push
        alert = None
        if attack == "ddos" or req_rate > 100:
            alert = {"client_ip": log["client_ip"], "attack_type": "DDoS_ATTACK",
                     "severity": "CRITICAL", "val": f"{req_rate} req/min",
                     "timestamp": datetime.now(timezone.utc).isoformat()}
        elif attack == "bruteforce" or log.get("status_code") == 401:
            alert = {"client_ip": log["client_ip"], "attack_type": "BRUTE_FORCE",
                     "severity": "HIGH", "val": f"{random.randint(28, 42)} failed logins/min",
                     "timestamp": datetime.now(timezone.utc).isoformat()}
        elif attack == "abuse":
            alert = {"client_ip": log["client_ip"], "attack_type": "ENDPOINT_ABUSE",
                     "severity": "MEDIUM", "val": f"{log.get('response_time_ms', 0)}ms avg lat",
                     "timestamp": datetime.now(timezone.utc).isoformat()}
        elif attack == "errors":
            alert = {"client_ip": log["client_ip"], "attack_type": "HTTP_5XX_SPIKE",
                     "severity": "MEDIUM", "val": "28% error rate",
                     "timestamp": datetime.now(timezone.utc).isoformat()}
        elif attack == "payload":
            alert = {"client_ip": log["client_ip"], "attack_type": "PAYLOAD_EXFILTRATION",
                     "severity": "HIGH", "val": f"{log.get('bytes_sent', 0) // 1048576:.1f} MB payload",
                     "timestamp": datetime.now(timezone.utc).isoformat()}

        if alert:
            STATE["alerts_triggered"] += 1
            STATE["recent_alerts"] = [alert] + STATE["recent_alerts"][:14]
            sev = alert["severity"]
            if sev in STATE["severity_counts"]:
                STATE["severity_counts"][sev] += 1

        # Push SSE events
        _push_sse_event("log", {
            "log": log_entry,
            "events_streamed": STATE["events_streamed"],
            "req_rate": req_rate,
            "window_labels": STATE["window_metrics"]["labels"],
            "window_data": STATE["window_metrics"]["req_counts"]
        })
        if alert:
            _push_sse_event("alert", {
                "alert": alert,
                "alerts_triggered": STATE["alerts_triggered"],
                "severity_counts": STATE["severity_counts"]
            })

        await asyncio.sleep(0.6)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/state")
async def get_state():
    return JSONResponse(content={
        "is_streaming": STATE["is_streaming"],
        "events_streamed": STATE["events_streamed"],
        "alerts_triggered": STATE["alerts_triggered"],
        "active_attack": STATE["active_attack"],
        "recent_logs": STATE["recent_logs"][:10],
        "recent_alerts": STATE["recent_alerts"][:10],
        "window_metrics": STATE["window_metrics"],
        "severity_counts": STATE["severity_counts"]
    })


@app.post("/api/toggle-stream")
async def toggle_stream(background_tasks: BackgroundTasks):
    STATE["is_streaming"] = not STATE["is_streaming"]
    if STATE["is_streaming"]:
        background_tasks.add_task(_simulation_loop)
    return JSONResponse(content={"is_streaming": STATE["is_streaming"]})


@app.post("/api/trigger-attack")
async def trigger_attack(attack_type: str, background_tasks: BackgroundTasks):
    import time
    STATE["active_attack"] = attack_type
    STATE["attack_expires_at"] = time.time() + 8.0  # 8-second burst

    # If simulator is not running, start it
    if not STATE["is_streaming"]:
        STATE["is_streaming"] = True
        background_tasks.add_task(_simulation_loop)

    # Immediately inject one synthetic alert for instant UI feedback
    alert_meta = {
        "ddos":       ("DDoS_ATTACK",         "CRITICAL", "140 req/min",         "192.168.1.250"),
        "bruteforce": ("BRUTE_FORCE",          "HIGH",     "35 failed logins/min","10.0.0.99"),
        "abuse":      ("ENDPOINT_ABUSE",       "MEDIUM",   "1,850ms avg lat",     "172.16.0.42"),
        "errors":     ("HTTP_5XX_SPIKE",       "MEDIUM",   "28% error rate",      "198.51.100.7"),
        "payload":    ("PAYLOAD_EXFILTRATION", "HIGH",     "18.4 MB payload",     "192.168.1.250")
    }
    if attack_type in alert_meta:
        atype, sev, val, ip = alert_meta[attack_type]
        alert = {"client_ip": ip, "attack_type": atype, "severity": sev,
                 "val": val, "timestamp": datetime.now(timezone.utc).isoformat()}
        STATE["alerts_triggered"] += 1
        STATE["recent_alerts"] = [alert] + STATE["recent_alerts"][:14]
        if sev in STATE["severity_counts"]:
            STATE["severity_counts"][sev] += 1
        _push_sse_event("alert", {
            "alert": alert,
            "alerts_triggered": STATE["alerts_triggered"],
            "severity_counts": STATE["severity_counts"]
        })

    return JSONResponse(content={"status": "attack_injected", "attack_type": attack_type})


@app.get("/api/stream")
async def sse_stream(request: Request):
    """
    Server-Sent Events endpoint.
    Browser connects once; the server pushes JSON event frames in real-time.
    Frame format:  data: {"type": "log"|"alert", "data": {...}}\n\n
    """
    q: asyncio.Queue = asyncio.Queue(maxsize=60)
    _sse_subscribers.append(q)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=20.0)
                    yield f"data: {payload}\n\n"
                except asyncio.TimeoutError:
                    # Send keep-alive comment
                    yield ": keep-alive\n\n"
        finally:
            if q in _sse_subscribers:
                _sse_subscribers.remove(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )


@app.post("/api/run-hive-query")
async def run_hive_query(request: Request):
    body = await request.json()
    sql = body.get("sql", "").strip()

    # Simulate varied responses based on query keywords
    if "severity" in sql.lower() or "critical" in sql.lower():
        rows = [r for r in HIVE_SIMULATED_ROWS if r["severity"] in ("CRITICAL", "HIGH")]
    elif "ddos" in sql.lower():
        rows = [r for r in HIVE_SIMULATED_ROWS if "DDoS" in r["attack_type"]]
    elif "brute" in sql.lower():
        rows = [r for r in HIVE_SIMULATED_ROWS if "BRUTE" in r["attack_type"]]
    else:
        rows = HIVE_SIMULATED_ROWS

    return JSONResponse(content={
        "status": "success",
        "query": sql,
        "rows": rows,
        "execution_time_ms": random.randint(140, 620),
        "engine": "Hive 2.3.2 on Tez"
    })


@app.post("/api/reset")
async def reset_state():
    """Reset all simulation counters."""
    STATE["is_streaming"] = False
    STATE["events_streamed"] = 0
    STATE["alerts_triggered"] = 0
    STATE["active_attack"] = None
    STATE["attack_expires_at"] = 0.0
    STATE["recent_logs"] = []
    STATE["recent_alerts"] = []
    STATE["window_metrics"] = {"labels": [], "req_counts": []}
    STATE["severity_counts"] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    return JSONResponse(content={"status": "reset_ok"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
