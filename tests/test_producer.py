import unittest
from producer.api_log_generator import generate_normal_log
from producer.attack_simulator import AttackSimulator

class TestProducer(unittest.TestCase):

    def test_generate_normal_log_structure(self):
        log = generate_normal_log()
        self.assertIn("timestamp", log)
        self.assertIn("request_id", log)
        self.assertIn("client_ip", log)
        self.assertIn("endpoint", log)
        self.assertIn("status_code", log)
        self.assertIn(log["status_code"], [200, 400, 404, 500])

    def test_attack_simulator_ddos(self):
        event = AttackSimulator.generate_ddos_event(target_ip="192.168.1.250")
        self.assertEqual(event["client_ip"], "192.168.1.250")
        self.assertEqual(event["endpoint"], "/api/v1/products")
        self.assertEqual(event["status_code"], 200)

    def test_attack_simulator_brute_force(self):
        event = AttackSimulator.generate_brute_force_event(target_ip="10.0.0.99")
        self.assertEqual(event["client_ip"], "10.0.0.99")
        self.assertEqual(event["endpoint"], "/api/v1/auth/login")
        self.assertEqual(event["status_code"], 401)

    def test_attack_simulator_payload_exfiltration(self):
        event = AttackSimulator.generate_large_payload_event(target_ip="192.168.1.250")
        self.assertGreater(event["bytes_sent"], 10000000)

if __name__ == "__main__":
    unittest.main()
