from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from sim.arrivals import Workload
from sim.metrics import validate_results
from sim.realtime import ROLES, RealtimeSimulation
from tests.helpers import config


def offered_counts(cfg):
    workload = Workload(cfg)
    warm = measured = 0
    while workload.next_time < cfg["warm_up_seconds"] + cfg["duration_seconds"]:
        when, _, _ = workload.pop()
        if when < cfg["warm_up_seconds"]:
            warm += 1
        else:
            measured += 1
    return warm, measured


class RuntimeTests(unittest.TestCase):
    def run_model(self, cfg, model):
        with redirect_stdout(io.StringIO()):
            simulation = RealtimeSimulation(cfg, model)
            result = simulation.run()
        self.assertTrue(all(not process.is_alive() for process in simulation.processes.values()))
        validate_results(result)
        return result

    def test_three_processes_cpu_affinity_and_exact_no_cache_latency(self):
        cfg = config(warm_up_seconds=1, duration_seconds=1, client_cache_size=0,
                     forwarder_cache_size=0, num_clients=3, num_forwarders=2)
        for model in ("baseline", "ReDNS"):
            with self.subTest(model=model):
                result = self.run_model(cfg, model)
                self.assertEqual((result["warm_up_queries"], result["total_queries"]), offered_counts(cfg))
                self.assertEqual(set(result["workers"]), set(ROLES))
                self.assertEqual(len({row["pid"] for row in result["workers"].values()}), 3)
                if hasattr(os, "sched_getaffinity") and len(os.sched_getaffinity(0)) >= 3:
                    self.assertEqual(len({row["cpu"] for row in result["workers"].values()}), 3)
                self.assertEqual(list(result["request_latencies_ms"]), [111] * result["total_queries"])
                self.assertEqual(result["authoritative_queries"], result["total_queries"])
                self.assertEqual(result["successful_reverse_queries"], 0)
                self.assertEqual(result["topology"], "client-forwarder-authority")
                self.assertNotIn("public_dns", result)

    def test_independent_auto_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "run_comparison"):
            RealtimeSimulation(config(time_scale="auto"), "ReDNS")

    def test_background_notifications_drain_at_fixed_scale(self):
        cfg = config(time_scale=0.1, client_cache_size=1,
                     forwarder_cache_size=1, ttl_range=[2, 2])
        result = self.run_model(cfg, "ReDNS")
        self.assertEqual(result["actual_time_scale"], cfg["time_scale"])
        self.assertEqual(result["total_queries"], offered_counts(cfg)[1])
        self.assertGreater(result["client"]["directory_stores_sent"], 0)
        self.assertGreater(result["average_wall_latency_ms"], 0)

    def test_concurrent_phases_policies_and_transfer_cohorts(self):
        for model in ("baseline", "ReDNS"):
            for policy in ("lru", "random"):
                with self.subTest(model=model, policy=policy):
                    cfg = config(
                        replacement_policy=policy, warm_up_seconds=1, duration_seconds=3,
                        queries_per_second=400, num_clients=12, num_forwarders=3, num_domains=80,
                        client_cache_size=2, forwarder_cache_size=3, time_scale=0.05,
                        regional_preferences={"enabled": True, "shared_rank_count": 8},
                        ttl_distribution={"values": [0, 1, 2, 100],
                                          "probabilities": [0.1, 0.3, 0.3, 0.3]},
                    )
                    result = self.run_model(cfg, model)
                    self.assertEqual((result["warm_up_queries"], result["total_queries"]), offered_counts(cfg))
                    max_children = (cfg["num_clients"] + cfg["num_forwarders"] - 1) // cfg["num_forwarders"]
                    local_costs = {cfg["client_forwarder_RTT"] * (1 + probes)
                                   for probes in range(max_children + 1)}
                    allowed = {0} | local_costs | {cost + cfg["forwarder_authoritative_RTT"]
                                                   for cost in local_costs}
                    self.assertTrue(set(result["request_latencies_ms"]) <= allowed)
                    self.assertGreater(result["client"][f"{policy}_evictions"], 0)
                    other = "random" if policy == "lru" else "lru"
                    self.assertEqual(result["client"][f"{other}_evictions"], 0)
                    if model == "baseline":
                        self.assertEqual(result["client"]["directory_stores_sent"], 0)
                    else:
                        self.assertGreater(result["client"]["directory_stores_sent"], 0)

    def test_backpressure_keeps_every_offered_request(self):
        cfg = config(duration_seconds=1, max_in_flight=1, time_scale=0.0001,
                     client_cache_size=0, forwarder_cache_size=0, timing={})
        result = self.run_model(cfg, "baseline")
        self.assertEqual(result["total_queries"], offered_counts(cfg)[1])
        self.assertGreater(result["arrival_lag_max_wall_ms"], 0)
        self.assertGreater(result["observed_simulated_seconds"], cfg["duration_seconds"])
        self.assertFalse(result["timing_valid"])

    def test_zero_load_and_optional_wall_latency_trace(self):
        for model in ("baseline", "ReDNS"):
            result = self.run_model(config(queries_per_second=0, time_scale=0.001,
                                           record_wall_latencies=False), model)
            self.assertEqual(result["total_queries"], 0)
            self.assertEqual(result["average_latency_ms"], 0)
            self.assertEqual(result["average_wall_latency_ms"], 0)
            self.assertNotIn("request_wall_latencies_ms", result)

    def test_result_validator_rejects_lost_queries(self):
        result = self.run_model(config(queries_per_second=10), "baseline")
        bad = deepcopy(result)
        bad["authoritative_queries"] += 1
        with self.assertRaises(RuntimeError):
            validate_results(bad)

    def test_only_baseline_and_redns_are_available(self):
        with self.assertRaises(ValueError):
            RealtimeSimulation(config(), "legacy")

    def test_scripts_and_modules_run_from_other_working_directory(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            cfg_path, output = Path(directory) / "config.json", Path(directory) / "result.json"
            cfg_path.write_text(json.dumps(config(queries_per_second=20)), encoding="utf-8")
            for script in ("run_baseline.py", "run_ReDNS.py"):
                completed = subprocess.run([
                    sys.executable, str(root / "sim" / script), "--config", str(cfg_path),
                    "--output", str(output), "--replacement-policy", "random", "--time-scale", "0.01",
                ], cwd=directory, capture_output=True, text=True, timeout=30)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                saved = json.loads(output.read_text())
                self.assertEqual(saved["config"]["replacement_policy"], "random")
                self.assertEqual(len(saved["workers"]), 3)
                self.assertIn("average_wall_latency_ms", completed.stdout)
                self.assertIn("successful_reverse_queries=", completed.stdout)
                self.assertEqual(saved["successful_reverse_queries"],
                                 sum(row["successful_reverse_queries"] for row in saved["forwarders"]))
            completed = subprocess.run([
                sys.executable, "-m", "sim.run_baseline", "--config", str(cfg_path),
                "--output", str(output),
            ], cwd=root, capture_output=True, text=True, timeout=30)
            self.assertEqual(completed.returncode, 0, completed.stderr)
