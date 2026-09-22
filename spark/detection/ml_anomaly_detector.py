import numpy as np
from sklearn.ensemble import IsolationForest

class MLAnomalyDetector:
    """Isolation Forest & Statistical Z-Score Machine Learning Anomaly Detector."""

    def __init__(self, contamination=0.05, n_estimators=100, random_state=42):
        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state
        )
        self.is_fitted = False
        self._seed_baseline_training()

    def _seed_baseline_training(self):
        """Seeds initial model with synthetic baseline normal traffic feature distributions."""
        # Features: [total_requests, failed_logins, error_5xx_count, avg_response_time, total_bytes]
        normal_samples = []
        for _ in range(500):
            reqs = np.random.randint(5, 35)
            failed = np.random.randint(0, 2)
            errors = np.random.randint(0, 1)
            resp = np.random.randint(20, 250)
            bytes_sent = np.random.randint(500, 50000)
            normal_samples.append([reqs, failed, errors, resp, bytes_sent])

        self.model.fit(normal_samples)
        self.is_fitted = True

    def predict_anomaly_score(self, feature_vector):
        """Returns anomaly score (0.0 to 1.0, where >0.65 indicates anomaly) and decision."""
        if not self.is_fitted:
            self._seed_baseline_training()

        X = np.array([feature_vector], dtype=float)
        # score_samples returns negative anomaly score (-1 for anomaly, 0 for normal)
        raw_score = float(self.model.score_samples(X)[0])
        # Map raw score [-1.0, 0.5] to normalized anomaly probability [0.0, 1.0]
        anomaly_score = max(0.0, min(1.0, round(0.5 - raw_score, 4)))
        is_anomaly = anomaly_score > 0.65
        return anomaly_score, is_anomaly

def evaluate_window_row(total_requests, failed_logins, error_5xx_count, avg_response_time, total_bytes):
    """Helper function to evaluate a single window metric tuple through Isolation Forest."""
    detector = MLAnomalyDetector()
    vec = [total_requests, failed_logins, error_5xx_count, avg_response_time, total_bytes]
    score, is_anom = detector.predict_anomaly_score(vec)
    return score, is_anom
