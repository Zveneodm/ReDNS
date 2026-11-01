from array import array
import json
from pathlib import Path
import tempfile
import unittest

from sim.config import DEFAULT_CONFIG, load_config, validate_config
from sim.experiment import write_results


class ConfigTests(unittest.TestCase):
    def test_project_config_uses_only_current_options(self):
        root = Path(__file__).resolve().parents[1]
        cfg = load_config(root / "config.json")
        supplied = json.loads((root / "config.json").read_text())
        for key, value in supplied.items():
            self.assertEqual(cfg[key], value)

    def test_removed_model_topology_and_learning_options_are_rejected(self):
        for key in ("pub_cache_size", "forwarder_pub_RTT", "pub_auth_RTT",
                    "redns_strategy", "min_transfer_ttl", "execution_mode"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_config({key: 1})
        for key in ("learning_half_life_seconds", "max_learned_domains_per_child"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_config({"regional_preferences": {key: 1}})

    def test_positive_counts_and_nonnegative_capacities(self):
        for key in ("duration_seconds", "num_clients", "num_forwarders", "num_domains",
                    "max_in_flight", "calibration_requests"):
            for value in (0, -1, True, 1.5):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    validate_config({key: value})
        for key in ("warm_up_seconds", "queries_per_second", "client_cache_size", "forwarder_cache_size"):
            self.assertEqual(validate_config({key: 0})[key], 0)
            with self.assertRaises(ValueError):
                validate_config({key: -1})

    def test_invalid_time_scale_rtt_and_flags(self):
        for key, values in {
            "time_scale": [0, -1, True, "fast", float("nan"), 1e-100],
            "min_time_scale": [0, "auto"],
            "client_forwarder_RTT": [-1, True, float("inf")],
            "forwarder_authoritative_RTT": [-1, False, float("nan")],
            "pin_cpu": [1], "record_wall_latencies": ["true"],
            "replacement_policy": ["fifo", [], None],
        }.items():
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    validate_config({key: value})

    def test_ttl_distribution_validation(self):
        for distribution in (
            {}, {"values": [], "probabilities": []},
            {"values": [-1], "probabilities": [1]},
            {"values": [10], "probabilities": [0.5]},
            {"values": [10], "probabilities": [float("nan")]},
            {"values": [10, 20], "probabilities": [1]},
        ):
            with self.subTest(distribution=distribution), self.assertRaises(ValueError):
                validate_config({"ttl_distribution": distribution})
        self.assertEqual(validate_config({"ttl_distribution": {
            "values": [0, 100], "probabilities": [0.1, 0.9],
        }})["ttl_distribution"]["values"], [0, 100])

    def test_timing_limits_are_strict_and_defaults_are_copied(self):
        for timing in (None, {"unknown": 1}, {"scale_multiplier": 1},
                       {"max_mean_lag_seconds": float("nan")},
                       {"consecutive_lag_windows": 0}, {"probe_duration_seconds": True},
                       {"max_attempts": 0}, {"probe_warm_up_seconds": -1}):
            with self.subTest(timing=timing), self.assertRaises(ValueError):
                validate_config({"timing": timing})
        cfg = validate_config({"timing": {"probe_warm_up_seconds": 0}})
        cfg["timing"]["max_attempts"] = 10
        self.assertEqual(DEFAULT_CONFIG["timing"]["max_attempts"], 4)

    def test_regional_workload_validation(self):
        for prefs in (None, {"enabled": 1}, {"shared_rank_count": 0},
                      {"enabled": True, "client_zipf_alpha": 1.1},
                      {"region_zipf_alpha": float("nan")}):
            with self.subTest(prefs=prefs), self.assertRaises(ValueError):
                validate_config({"regional_preferences": prefs})
        with self.assertRaises(ValueError):
            validate_config({"query_distribution": "uniform", "regional_preferences": {"enabled": True}})

    def test_validation_does_not_mutate_defaults_or_supplied_lists(self):
        cfg = validate_config({})
        cfg["ttl_range"][0] = 999
        cfg["regional_preferences"]["enabled"] = True
        self.assertEqual(DEFAULT_CONFIG["ttl_range"], [30, 300])
        self.assertFalse(DEFAULT_CONFIG["regional_preferences"]["enabled"])

    def test_bom_config_loading_and_streaming_array_output(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text('{"num_clients": 3}', encoding="utf-8-sig")
            self.assertEqual(load_config(path)["num_clients"], 3)
            output = Path(directory) / "result.json"
            values = array("d", range(20000))
            write_results(output, {"request_latencies_ms": values, "experiment": "ReDNS"})
            self.assertEqual(json.loads(output.read_text())["request_latencies_ms"], values.tolist())
