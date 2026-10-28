from collections import Counter
from contextlib import redirect_stdout
import io
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from sim.arrivals import Workload
from sim.clock import RealtimeClock
from sim.experiment import validate_config, write_results
from sim.realtime import RealtimeSimulation
from src.baseline.Record import Record as BaselineRecord
from src.Re_DNS.Record import Record as ReDNSRecord


class RealtimeTests(unittest.TestCase):
    def config(self, **overrides):
        return validate_config({
            "warm_up_seconds": 1, "duration_seconds": 1, "queries_per_second": 200,
            "num_clients": 3, "num_forwarders": 2, "num_domains": 20,
            "client_cache_size": 0, "forwarder_cache_size": 0, "pub_cache_size": 0,
            "ttl_range": [60, 60], "time_scale": 0.02, "max_in_flight": 64,
            "calibration_requests": 50, **overrides,
        })

    @staticmethod
    def offered_counts(config):
        workload = Workload(config)
        warm = measured = 0
        end = config["warm_up_seconds"] + config["duration_seconds"]
        while workload.next_time < end:
            when, _, _ = workload.pop()
            if when < config["warm_up_seconds"]:
                warm += 1
            else:
                measured += 1
        return warm, measured

    def test_poisson_counts_and_zipf_domains(self):
        config = self.config(num_domains=20, queries_per_second=20, zipf_alpha=1.2)
        workload = Workload(config)
        bins = [0] * 2000
        domains = Counter()
        clients = Counter()
        while workload.next_time < len(bins):
            when, client, domain = workload.pop()
            bins[int(when)] += 1
            domains[domain] += 1
            clients[client] += 1
        self.assertAlmostEqual(statistics.mean(bins), 20, delta=0.4)
        self.assertAlmostEqual(statistics.pvariance(bins), 20, delta=2)
        normalization = sum(rank ** -1.2 for rank in range(1, 21))
        self.assertAlmostEqual(domains[0] / sum(domains.values()), 1 / normalization, delta=0.012)
        for count in clients.values():
            self.assertAlmostEqual(count / sum(clients.values()), 1 / 3, delta=0.012)

    def test_workload_seed_and_zero_rate(self):
        first, second = Workload(self.config()), Workload(self.config())
        self.assertEqual([first.pop() for _ in range(100)], [second.pop() for _ in range(100)])
        self.assertEqual(Workload(self.config(queries_per_second=0)).next_time, math.inf)

    def test_scaled_clock_and_fractional_ttl(self):
        for record_class in (BaselineRecord, ReDNSRecord):
            with self.subTest(record=record_class.__module__):
                clock = RealtimeClock()
                self.assertEqual(clock.now, 0)
                clock.start(1_000_000_000, 0.0001)
                with patch("sim.clock.perf_counter_ns", return_value=1_000_050_000):
                    self.assertAlmostEqual(clock.now, 0.5)
                    record = record_class("a", 0.75, clock=clock)
                    self.assertAlmostEqual(record._expires_at, 1.25)
                with patch("sim.clock.perf_counter_ns", return_value=1_000_100_000):
                    self.assertAlmostEqual(record.ttl, 0.25)
                    self.assertAlmostEqual(record.copy().ttl, 0.25)
                clock.freeze(1_000_125_000)
                self.assertTrue(record.is_expired())
                self.assertEqual(record.auth_ttl, 0.75)

    def test_four_process_layers_and_foreground_latencies(self):
        config = self.config()
        expected_warm, expected_count = self.offered_counts(config)
        for model in ("baseline", "ReDNS"):
            with self.subTest(model=model), redirect_stdout(io.StringIO()):
                simulation = RealtimeSimulation(config, model)
                result = simulation.run()
                self.assertEqual(result["total_queries"], expected_count)
                self.assertEqual(result["warm_up_queries"], expected_warm)
                self.assertEqual(list(result["request_latencies_ms"]), [61] * expected_count)
                self.assertEqual(len(result["request_wall_latencies_ms"]), expected_count)
                self.assertGreater(result["average_wall_latency_ms"], 0)
                self.assertEqual(result["average_latency_ms"], 61)
                for layer in ("client", "forwarder", "public_dns"):
                    self.assertEqual(result[layer]["queries"], expected_count)
                    self.assertEqual(result[layer]["misses"], expected_count)
                self.assertEqual(result["authoritative_queries"], expected_count)
                self.assertEqual(len({row["pid"] for row in result["workers"].values()}), 4)
                if hasattr(os, "sched_getaffinity") and len(os.sched_getaffinity(0)) >= 4:
                    self.assertEqual(len({row["cpu"] for row in result["workers"].values()}), 4)
                self.assertTrue(all(not process.is_alive() for process in simulation.processes.values()))
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "results.json"
                    write_results(path, result)
                    saved = json.loads(path.read_text(encoding="utf-8"))
                    self.assertEqual(saved["request_latencies_ms"], [61] * expected_count)

    def test_automatic_scaling_and_background_drain(self):
        config = self.config(
            warm_up_seconds=0, duration_seconds=2, queries_per_second=40,
            num_clients=2, num_forwarders=1, num_domains=10,
            client_cache_size=1, forwarder_cache_size=1, pub_cache_size=1,
            ttl_range=[2, 2], min_transfer_ttl=3, time_scale="auto", min_time_scale=0.01,
        )
        with redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(config, "ReDNS").run()
        self.assertGreater(result["calibrated_queries_per_wall_second"], 0)
        self.assertGreaterEqual(result["actual_time_scale"], 0.01)
        self.assertEqual(result["total_queries"], self.offered_counts(config)[1])
        self.assertTrue(set(result["request_latencies_ms"]) <= {0, 1, 11, 61})
        c, f, p = (result[key] for key in ("client", "forwarder", "public_dns"))
        self.assertEqual(c["upstream_queries"], f["queries"])
        self.assertEqual(f["upstream_queries"], p["queries"])
        self.assertEqual(p["upstream_queries"], result["authoritative_queries"])
        self.assertEqual(p["records_transferred"], f["records_received"])
        self.assertEqual(f["records_transferred"], c["records_received"])
        self.assertGreater(p["records_transferred"], 0)
        self.assertGreater(p["refresh_queries"], 0)
        for layer in (c, f, p):
            self.assertEqual(layer["queries"], layer["hits"] + layer["misses"])
            self.assertEqual(layer["upstream_queries"], layer["misses"] + layer["refresh_queries"])

    def test_regional_preferences_are_learned_from_forwarded_misses(self):
        config = self.config(
            warm_up_seconds=0, duration_seconds=2, queries_per_second=80,
            num_clients=6, num_forwarders=2, num_domains=40,
            client_cache_size=2, forwarder_cache_size=2, pub_cache_size=2,
            time_scale=0.01,
            regional_preferences={
                "enabled": True, "region_zipf_alpha": 1.1,
                "client_zipf_alpha": 1.5, "shared_rank_count": 8,
                "learning_half_life_seconds": 30.0,
                "max_learned_domains_per_child": 16,
            },
        )
        with redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(config, "ReDNS").run()
        public = result["public_dns_preference_learning"]
        forwarders = result["forwarder_preference_learning"]
        self.assertGreater(public["observed_requests"], 0)
        self.assertGreater(forwarders["observed_requests"], public["observed_requests"])
        self.assertEqual(public["fit_scope"], "forwarded_cache_misses")
        self.assertGreater(result["public_dns"]["records_transferred"], 0)
        self.assertEqual(result["public_dns"]["records_transferred"],
                         result["forwarder"]["records_received"])

    def test_zero_workload_and_optional_wall_trace(self):
        with redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(self.config(
                queries_per_second=0, warm_up_seconds=0, time_scale=0.001,
                record_wall_latencies=False,
            )).run()
        self.assertEqual(result["total_queries"], 0)
        self.assertEqual(result["average_latency_ms"], 0)
        self.assertEqual(result["average_wall_latency_ms"], 0)
        self.assertNotIn("request_wall_latencies_ms", result)

    def test_invalid_realtime_options(self):
        for field, value in (
            ("time_scale", 0), ("time_scale", True), ("time_scale", float("nan")),
            ("time_scale", 1e-100),
            ("time_scale", "fast"), ("min_time_scale", -1), ("min_time_scale", "auto"),
            ("max_in_flight", 0), ("calibration_requests", 0),
            ("pin_cpu", 1), ("record_wall_latencies", "yes"), ("execution_mode", "threads"),
        ):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate_config({field: value})

    def test_backpressure_preserves_all_offered_requests(self):
        config = self.config(warm_up_seconds=0, queries_per_second=100,
                             time_scale=0.0001, max_in_flight=1)
        with redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(config).run()
        self.assertEqual(result["total_queries"], self.offered_counts(config)[1])
        self.assertGreater(result["arrival_lag_max_wall_ms"], 0)
        self.assertGreater(result["observed_simulated_seconds"], config["duration_seconds"])
        self.assertEqual(result["authoritative_queries"], result["total_queries"])

    def test_cli_entry_points_and_reference_override(self):
        root = Path(__file__).resolve().parents[1]
        config = self.config(queries_per_second=20, calibration_requests=20)
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            output_path = Path(directory) / "result.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            for script, mode, scale in (("run_baseline.py", "realtime", "0.02"),
                                        ("run_ReDNS.py", "realtime", "auto"),
                                        ("run_baseline.py", "deterministic", "0.02")):
                with self.subTest(script=script, mode=mode):
                    process = subprocess.run(
                        [sys.executable, str(root / "sim" / script), "--config", str(config_path),
                         "--output", str(output_path), "--execution-mode", mode, "--time-scale", scale],
                        capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(process.returncode, 0, process.stderr)
                    self.assertNotIn("Realtime:", process.stdout)
                    self.assertNotIn("four layer processes", process.stdout)
                    self.assertIn("2s/2s", process.stdout)
                    result = json.loads(output_path.read_text(encoding="utf-8"))
                    self.assertEqual(result["config"]["execution_mode"], mode)
                    self.assertEqual(len(result["request_latencies_ms"]), result["total_queries"])
                    if mode == "realtime":
                        self.assertEqual(result["total_queries"], self.offered_counts(config)[1])
                        self.assertIn("average_wall_latency_ms", process.stdout)
                        self.assertEqual(len(result["workers"]), 4)
                    else:
                        self.assertEqual(result["total_queries"], 20)


if __name__ == "__main__":
    unittest.main()
