"""Strict configuration for the multiprocessing DNS experiments."""

import json
import math
from pathlib import Path
from time import get_clock_info

from src.replacement import validate_replacement_policy


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = {
    "seed": 42,
    "time_scale": 2.0,
    "min_time_scale": 0.0001,
    "max_in_flight": 512,
    "calibration_requests": 1000,
    "timing": {
        "max_mean_lag_seconds": 0.01,
        "max_window_lag_seconds": 0.05,
        "consecutive_lag_windows": 3,
        "max_overrun_seconds": 0.1,
        "probe_warm_up_seconds": 20,
        "probe_duration_seconds": 30,
        "max_attempts": 4,
        "scale_multiplier": 2.0,
        "auto_initial_time_scale": 2.0,
    },
    "pin_cpu": True,
    "workers": {"client": 1, "forwarder": 1, "manager": 1},
    "prefetch": {
        "report_seconds": 1.0, "history_seconds": 300.0, "advice_seconds": 5.0,
        "alpha": 1, "beta": 0, "gamma": 1.0, "epsilon": None, "zeta": None,
        "max_per_forwarder": 8,
    },
    "record_wall_latencies": True,
    "warm_up_seconds": 0,
    "duration_seconds": 300,
    "queries_per_second": 100,
    "num_clients": 100,
    "num_forwarders": 5,
    "forwarder_cluster_size": 1,
    "client_cache_size": 100,
    "forwarder_cache_size": 1000,
    "replacement_policy": "lru",
    "num_domains": 2000,
    "ttl_range": [30, 300],
    "ttl_distribution": None,
    "query_distribution": "zipf",
    "zipf_alpha": 1.1,
    "regional_preferences": {
        "enabled": False,
        "region_zipf_alpha": 1.2,
        "client_zipf_alpha": 1.5,
        "shared_rank_count": 64,
    },
    "output_dir": "res",
    "client_forwarder_RTT": 1,
    "forwarder_forwarder_RTT": 2,
    "forwarder_manager_RTT": 1,
    "forwarder_authoritative_RTT": 110,
}


def validate_config(config):
    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")
    unknown = config.keys() - DEFAULT_CONFIG.keys()
    if unknown:
        raise ValueError(f"unknown config fields: {', '.join(sorted(unknown))}")
    result = {**DEFAULT_CONFIG, **config}
    prefetch = config.get("prefetch", {})
    if not isinstance(prefetch, dict) or prefetch.keys() - DEFAULT_CONFIG["prefetch"].keys():
        raise ValueError("prefetch must contain only supported fields")
    result["prefetch"] = {**DEFAULT_CONFIG["prefetch"], **prefetch}
    for name, value in result["prefetch"].items():
        if name in ("alpha", "beta", "epsilon", "zeta", "max_per_forwarder"):
            if name in ("epsilon", "zeta") and value is None:
                continue
            minimum = 1 if name in ("alpha", "max_per_forwarder") else 0
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"prefetch.{name} must be an integer >= {minimum}")
        elif (isinstance(value, bool) or not isinstance(value, (int, float))
              or not math.isfinite(value) or value <= 0):
            raise ValueError(f"prefetch.{name} must be finite and positive")
    workers = config.get("workers", {})
    if not isinstance(workers, dict) or workers.keys() - DEFAULT_CONFIG["workers"].keys():
        raise ValueError("workers must contain only client, forwarder and manager counts")
    result["workers"] = {**DEFAULT_CONFIG["workers"], **workers}
    for name, value in result["workers"].items():
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"workers.{name} must be a positive integer")
    timing = config.get("timing", {})
    if not isinstance(timing, dict) or timing.keys() - DEFAULT_CONFIG["timing"].keys():
        raise ValueError("timing must be an object containing only supported fields")
    result["timing"] = {**DEFAULT_CONFIG["timing"], **timing}
    for name, value in result["timing"].items():
        if name in ("consecutive_lag_windows", "probe_warm_up_seconds",
                    "probe_duration_seconds", "max_attempts"):
            minimum = 0 if name == "probe_warm_up_seconds" else 1
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"timing.{name} must be an integer >= {minimum}")
        elif (isinstance(value, bool) or not isinstance(value, (int, float))
              or not math.isfinite(value) or value <= 0):
            raise ValueError(f"timing.{name} must be finite and positive")
    if result["timing"]["scale_multiplier"] <= 1:
        raise ValueError("timing.scale_multiplier must exceed 1")
    validate_replacement_policy(result["replacement_policy"])
    supplied_preferences = config.get("regional_preferences", {})
    if not isinstance(supplied_preferences, dict):
        raise ValueError("regional_preferences must be an object")
    result["regional_preferences"] = {
        **DEFAULT_CONFIG["regional_preferences"],
        **supplied_preferences,
    }
    positive = ("duration_seconds", "num_clients", "num_forwarders", "num_domains",
                "max_in_flight", "calibration_requests", "forwarder_cluster_size")
    nonnegative = (
        "queries_per_second", "client_cache_size", "forwarder_cache_size",
        "warm_up_seconds",
    )
    for name in ("seed",) + positive + nonnegative:
        value = result[name]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer")
        if name in positive and value <= 0:
            raise ValueError(f"{name} must be positive")
        if name in nonnegative and value < 0:
            raise ValueError(f"{name} must be non-negative")
    for name in ("client_forwarder_RTT", "forwarder_forwarder_RTT", "forwarder_manager_RTT",
                 "forwarder_authoritative_RTT"):
        value = result[name]
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0):
            raise ValueError(f"{name} must be finite and non-negative (milliseconds)")
    ttl_range = result["ttl_range"]
    if (not isinstance(ttl_range, list) or len(ttl_range) != 2
            or any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in ttl_range)
            or ttl_range[0] > ttl_range[1]):
        raise ValueError("ttl_range must be [min, max] with non-negative integer bounds")
    result["ttl_range"] = list(ttl_range)
    for name in ("time_scale", "min_time_scale"):
        value = result[name]
        if name == "time_scale" and value == "auto":
            continue
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0):
            raise ValueError(f"{name} must be finite and positive; time_scale also accepts 'auto'")
    for name in ("pin_cpu", "record_wall_latencies"):
        if not isinstance(result[name], bool):
            raise ValueError(f"{name} must be a boolean")
    if (result["time_scale"] != "auto"
            and result["time_scale"] < get_clock_info("perf_counter").resolution
            * max(1, result["queries_per_second"])):
        raise ValueError("time_scale is too small to resolve the mean arrival interval with this clock")
    distribution = result["ttl_distribution"]
    if distribution is not None:
        if not isinstance(distribution, dict) or distribution.keys() != {"values", "probabilities"}:
            raise ValueError("ttl_distribution must contain values and probabilities")
        values = distribution["values"]
        probabilities = distribution["probabilities"]
        if (not isinstance(values, list) or not values
                or any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in values)):
            raise ValueError("ttl_distribution.values must be a non-empty list of non-negative integers")
        if (not isinstance(probabilities, list) or len(probabilities) != len(values)
                or any(isinstance(p, bool) or not isinstance(p, (int, float))
                       or not math.isfinite(p) or not 0 <= p <= 1 for p in probabilities)):
            raise ValueError("ttl_distribution.probabilities must match values and contain probabilities in [0, 1]")
        if not math.isclose(math.fsum(probabilities), 1.0, rel_tol=0, abs_tol=1e-9):
            raise ValueError("ttl_distribution.probabilities must sum to 1")
        result["ttl_distribution"] = {
            "values": list(values), "probabilities": list(probabilities),
        }
    if result["query_distribution"] not in ("uniform", "zipf"):
        raise ValueError("query_distribution must be 'uniform' or 'zipf'")
    alpha = result["zipf_alpha"]
    if isinstance(alpha, bool) or not isinstance(alpha, (int, float)) or not 0 < alpha < float("inf"):
        raise ValueError("zipf_alpha must be finite and positive")
    preferences = result["regional_preferences"]
    allowed_preferences = set(DEFAULT_CONFIG["regional_preferences"])
    if not isinstance(preferences, dict) or preferences.keys() - allowed_preferences:
        raise ValueError("regional_preferences contains unknown fields")
    if not isinstance(preferences["enabled"], bool):
        raise ValueError("regional_preferences.enabled must be a boolean")
    if preferences["enabled"] and result["query_distribution"] != "zipf":
        raise ValueError("regional_preferences requires query_distribution='zipf'")
    for name in ("region_zipf_alpha", "client_zipf_alpha"):
        value = preferences[name]
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0):
            raise ValueError(f"regional_preferences.{name} must be finite and positive")
    if (preferences["enabled"] and
            preferences["client_zipf_alpha"] <= preferences["region_zipf_alpha"]):
        raise ValueError("regional_preferences.client_zipf_alpha must exceed region_zipf_alpha")
    for name in ("shared_rank_count",):
        value = preferences[name]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"regional_preferences.{name} must be a positive integer")
    if not isinstance(result["output_dir"], str) or not result["output_dir"].strip():
        raise ValueError("output_dir must be a non-empty path string")
    return result


def load_config(path):
    with Path(path).open(encoding="utf-8-sig") as stream:
        return validate_config(json.load(stream))
