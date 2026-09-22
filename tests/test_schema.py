import unittest
from spark.schemas.api_schema import get_api_log_schema, get_alert_schema

class TestSchemas(unittest.TestCase):

    def test_api_log_schema_fields(self):
        schema = get_api_log_schema()
        if hasattr(schema, "fields"):
            field_names = [f.name for f in schema.fields]
        else:
            field_names = schema
        self.assertIn("timestamp", field_names)
        self.assertIn("client_ip", field_names)
        self.assertIn("endpoint", field_names)
        self.assertIn("status_code", field_names)
        self.assertIn("bytes_sent", field_names)

    def test_alert_schema_fields(self):
        schema = get_alert_schema()
        if hasattr(schema, "fields"):
            field_names = [f.name for f in schema.fields]
        else:
            field_names = schema
        self.assertIn("window_start", field_names)
        self.assertIn("attack_type", field_names)
        self.assertIn("severity", field_names)
        self.assertIn("anomaly_score", field_names)

if __name__ == "__main__":
    unittest.main()
