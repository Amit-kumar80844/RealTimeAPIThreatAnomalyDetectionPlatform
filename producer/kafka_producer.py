import sys
import time
import json
import argparse
import logging
from producer.api_log_generator import generate_normal_log
from producer.attack_simulator import AttackSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class LogProducerManager:
    """Manages high-throughput API log generation and Kafka delivery."""

    def __init__(self, bootstrap_servers="localhost:9092", topic="api-raw-logs"):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._init_kafka()

    def _init_kafka(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                retries=3
            )
            logging.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
        except Exception as e:
            logging.warning(f"Kafka broker not reachable ({e}). Running in mock output mode.")
            self.producer = None

    def send_event(self, event):
        key = event.get("client_ip", "default_key")
        if self.producer:
            self.producer.send(self.topic, key=key, value=event)
        return event

    def flush(self):
        if self.producer:
            self.producer.flush()

    def run_stream(self, rate_per_sec=100, attack_scenario=None, duration_sec=None):
        """Continuously streams events with specified traffic rate and attack mode."""
        start_time = time.time()
        sent_count = 0

        logging.info(f"Starting API Log Stream -> Topic: {self.topic} | Target Rate: {rate_per_sec} events/sec")
        if attack_scenario:
            logging.info(f"🔥 Active Attack Injection Mode: {attack_scenario.upper()}")

        try:
            while True:
                batch_size = max(1, rate_per_sec // 10)
                for _ in range(batch_size):
                    if attack_scenario == "ddos":
                        event = AttackSimulator.generate_ddos_event()
                    elif attack_scenario == "bruteforce":
                        event = AttackSimulator.generate_brute_force_event()
                    elif attack_scenario == "abuse":
                        event = AttackSimulator.generate_endpoint_abuse_event()
                    elif attack_scenario == "errors":
                        event = AttackSimulator.generate_error_spike_event()
                    elif attack_scenario == "payload":
                        event = AttackSimulator.generate_large_payload_event()
                    elif attack_scenario == "botnet":
                        event = AttackSimulator.generate_distributed_ddos_event()
                    else:
                        event = generate_normal_log()

                    self.send_event(event)
                    sent_count += 1

                self.flush()
                time.sleep(0.1)

                if duration_sec and (time.time() - start_time) >= duration_sec:
                    break
        except KeyboardInterrupt:
            logging.info("Stream interrupted by user.")
        
        logging.info(f"Completed streaming. Total events produced: {sent_count}")
        return sent_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-Time API Log Generator & Kafka Producer")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="api-raw-logs", help="Kafka topic name")
    parser.add_argument("--rate", type=int, default=100, help="Events per second")
    parser.add_argument("--attack", choices=["ddos", "bruteforce", "abuse", "errors", "payload", "botnet"], help="Attack scenario type")
    parser.add_argument("--duration", type=int, help="Duration in seconds (optional)")

    args = parser.parse_args()
    manager = LogProducerManager(bootstrap_servers=args.broker, topic=args.topic)
    manager.run_stream(rate_per_sec=args.rate, attack_scenario=args.attack, duration_sec=args.duration)
