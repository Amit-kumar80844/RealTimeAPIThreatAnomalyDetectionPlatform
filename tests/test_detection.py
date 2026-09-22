import unittest
from spark.utils.metrics import calculate_z_score, calculate_stats

class TestDetectionMetrics(unittest.TestCase):

    def test_calculate_z_score(self):
        self.assertEqual(calculate_z_score(100, 40, 10), 6.0)
        self.assertEqual(calculate_z_score(40, 40, 10), 0.0)
        self.assertEqual(calculate_z_score(50, 40, 0), 0.0)

    def test_calculate_stats(self):
        mean, stddev = calculate_stats([10, 20, 30, 40, 50])
        self.assertEqual(mean, 30.0)
        self.assertEqual(round(stddev, 2), 15.81)

if __name__ == "__main__":
    unittest.main()
