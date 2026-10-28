"""Configuration and result IO shared by the experiment entry points."""

import argparse
from array import array
import json
import math
from pathlib import Path
from time import get_clock_info


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = {
    "seed": 42,
    "execution_mode": "realtime",
    "time_scale": "auto",
    "min_time_scale": 0.0001,
    "max_in_flight": 512,
    "calibration_requests": 1000,
    "pin_cpu": True,
    "record_wall_latencies": True,
    "warm_up_seconds": 0,
    "duration_seconds": 300,
    "queries_per_second": 100,
    "num_clients": 100,
    "num_forwarders": 5,
    "client_cache_size": 100,
    "forwarder_cache_size": 1000,
    "pub_cache_size": 10000,
    "min_transfer_ttl": 30,
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
        "learning_half_life_seconds": 600.0,
        "max_learned_domains_per_child": 64,
    },
    "output_dir": "res",
    "client_forwarder_RTT": 1,
    "forwarder_pub_RTT": 10,
    "pub_auth_RTT": 50,
}


def validate_config(config):
    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")
    unknown = config.keys() - DEFAULT_CONFIG.keys()
    if unknown:
        raise ValueError(f"unknown config fields: {', '.join(sorted(unknown))}")
    result = {**DEFAULT_CONFIG, **config}
    supplied_preferences = config.get("regional_preferences", {})
    if not isinstance(supplied_preferences, dict):
        raise ValueError("regional_preferences must be an object")
    result["regional_preferences"] = {
        **DEFAULT_CONFIG["regional_preferences"],
        **supplied_preferences,
    }
    positive = ("duration_seconds", "num_clients", "num_forwarders", "num_domains",
                "max_in_flight", "calibration_requests")
    nonnegative = (
        "queries_per_second", "client_cache_size", "forwarder_cache_size",
        "pub_cache_size", "min_transfer_ttl", "warm_up_seconds",
    )
    for name in ("seed",) + positive + nonnegative:
        value = result[name]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer")
        if name in positive and value <= 0:
            raise ValueError(f"{name} must be positive")
        if name in nonnegative and value < 0:
            raise ValueError(f"{name} must be non-negative")
    for name in ("client_forwarder_RTT", "forwarder_pub_RTT", "pub_auth_RTT"):
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
    if result["execution_mode"] not in ("realtime", "deterministic"):
        raise ValueError("execution_mode must be 'realtime' or 'deterministic'")
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
    if (result["execution_mode"] == "realtime" and result["time_scale"] != "auto"
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
    for name in ("region_zipf_alpha", "client_zipf_alpha", "learning_half_life_seconds"):
        value = preferences[name]
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0):
            raise ValueError(f"regional_preferences.{name} must be finite and positive")
    if (preferences["enabled"] and
            preferences["client_zipf_alpha"] <= preferences["region_zipf_alpha"]):
        raise ValueError("regional_preferences.client_zipf_alpha must exceed region_zipf_alpha")
    for name in ("shared_rank_count", "max_learned_domains_per_child"):
        value = preferences[name]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"regional_preferences.{name} must be a positive integer")
    if not isinstance(result["output_dir"], str) or not result["output_dir"].strip():
        raise ValueError("output_dir must be a non-empty path string")
    return result


def load_config(path):
    with Path(path).open(encoding="utf-8-sig") as stream:
        return validate_config(json.load(stream))


def write_results(path, results):
    """Stream large per-request arrays without making a second full list."""
    with Path(path).open("w", encoding="utf-8") as stream:
        stream.write("{\n")
        for index, (key, value) in enumerate(results.items()):
            if index:
                stream.write(",\n")
            stream.write("  " + json.dumps(key) + ": ")
            if isinstance(value, array):
                stream.write("[")
                for start in range(0, len(value), 8192):
                    if start:
                        stream.write(",")
                    stream.write("\n    " + json.dumps(value[start:start + 8192].tolist())[1:-1])
                stream.write("\n  ]" if value else "]")
            else:
                stream.write(json.dumps(value, ensure_ascii=False, indent=2).replace("\n", "\n  "))
        stream.write("\n}\n")


def _time_scale(value):
    return "auto" if value == "auto" else float(value)


def run_experiment(run_simulation, label, result_filename, argv=None):
    parser = argparse.ArgumentParser(description=f"Run the {label} DNS experiment")
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / "config.json")
    parser.add_argument("--output", type=Path, help="Override the JSON results path")
    parser.add_argument("--execution-mode", choices=("realtime", "deterministic"))
    parser.add_argument("--time-scale", type=_time_scale,
                        help="Wall seconds per simulated second, or 'auto'")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.execution_mode is not None:
            config["execution_mode"] = args.execution_mode
        if args.time_scale is not None:
            config["time_scale"] = args.time_scale
        config = validate_config(config)
        output = args.output or (PROJECT_ROOT / config["output_dir"] / result_filename)
        results = run_simulation(config)
        output.parent.mkdir(parents=True, exist_ok=True)
        write_results(output, results)
    except (OSError, ValueError, NotImplementedError, RuntimeError) as exc:
        parser.error(str(exc))
    print(f"{label}: {results['total_queries']} queries over {results['elapsed_seconds']} simulated seconds")
    for layer in ("client", "forwarder", "public_dns"):
        stats = results[layer]
        print(f"{layer}: queries={stats['queries']}, hits={stats['hits']}, "
              f"misses={stats['misses']}, hit_rate={stats['hit_rate']:.2%}")
    print(f"authoritative_queries={results['authoritative_queries']}")
    print(f"average_latency_ms={results['average_latency_ms']:.6f}")
    if results.get("execution_mode") == "realtime":
        print(f"average_wall_latency_ms={results['average_wall_latency_ms']:.6f}")
        print(f"actual_time_scale={results['actual_time_scale']:.9g}")
        print(f"arrival_lag_mean_wall_ms={results['arrival_lag_mean_wall_ms']:.6f}")
    print(f"Results: {output.resolve()}")
    return 0
