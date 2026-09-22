import math
import numpy as np

def calculate_z_score(value, mean, stddev):
    """Calculates Z-score for a given value relative to historical baseline statistics."""
    if stddev == 0 or stddev is None or math.isnan(stddev):
        return 0.0
    return (value - mean) / stddev

def calculate_stats(values):
    """Computes mean and sample standard deviation for a list of numerical values."""
    if not values:
        return 0.0, 0.0
    arr = np.array(values, dtype=float)
    mean = float(np.mean(arr))
    stddev = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    return mean, stddev

def normalize_feature_vector(feature_vector, feature_ranges):
    """Normalizes multi-dimensional feature vector into [0, 1] range."""
    normalized = []
    for val, (min_val, max_val) in zip(feature_vector, feature_ranges):
        if max_val == min_val:
            normalized.append(0.0)
        else:
            norm = (val - min_val) / (max_val - min_val)
            normalized.append(max(0.0, min(1.0, norm)))
    return normalized
