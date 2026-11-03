from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from lib.comparison import run_comparison, validate_pair
from lib.config import DEFAULT_CONFIG
from lib.experiment import run_experiment
from lib.realtime import RealtimeSimulation
from lib.timing import SimulationOverload, TimingMonitor
from tests.helpers import config


class TimingTests(unittest.TestCase):
    def monitor(self):
        return TimingMonitor(DEFAULT_CONFIG["timing"])

    def test_sustained_lag_and_warmup_are_checked(self):
        monitor = self.monitor()
        for second in range(4):
            monitor.observe(second + 0.1, 0.2)
        self.assertTrue(monitor.sustained)
        report = monitor.report(finish=True)
        self.assertFalse(report["valid"])
        self.assertEqual(report["scheduled_queries"], 4)
        self.assertEqual(report["longest_lag_streak"], 4)

    def test_single_spike_is_not_sustained_and_empty_windows_break_streak(self):
        monitor = self.monitor()
        monitor.observe(0.1, 0.06)
        monitor.observe(1.1, 0.06)
        monitor.observe(4.1, 0.06)
        for i in range(100):
            monitor.observe(5.1 + i / 1000, 0.001)
        report = monitor.report(finish=True)
        self.assertTrue(report["valid"])
        self.assertEqual(report["longest_lag_streak"], 2)
        self.assertFalse(monitor.sustained)

    def test_global_mean_and_drain_overrun_are_independent_gates(self):
        monitor = self.monitor()
        monitor.observe(0.1, 0.02)
        self.assertFalse(monitor.report(finish=True)["valid"])
        monitor = self.monitor()
        monitor.observe(0.1, 0)
        self.assertFalse(monitor.report(overrun=0.5, finish=True)["valid"])

    def test_real_overload_can_abort_and_cleans_up_workers(self):
        cfg = config(duration_seconds=4, queries_per_second=100, time_scale=0.001,
                     max_in_flight=1, client_cache_size=0, forwarder_cache_size=0, timing={})
        simulation = RealtimeSimulation(cfg, abort_on_overload=True)
        with redirect_stdout(io.StringIO()), self.assertRaises(SimulationOverload):
            simulation.run()
        self.assertTrue(all(not process.is_alive() for process in simulation.processes.values()))

    def test_single_cli_does_not_overwrite_results_from_invalid_run(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "existing.json"
            output.write_text('{"previous": true}')
            invalid = {"timing_valid": False, "timing_validation": {"reasons": ["backlog"]}}
            with patch("lib.experiment.load_config", return_value=config()), \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                run_experiment(lambda cfg: invalid, "test", ["--output", str(output)])
            self.assertEqual(json.loads(output.read_text()), {"previous": True})


def fake_result(cfg, model, cpu_pool):
    """Only publication metadata; the real-process test below checks protocols."""
    return dict(experiment=model, config=deepcopy(cfg), actual_time_scale=cfg["time_scale"],
                cpu_pool=list(cpu_pool), workers={role: {"cpu": i} for i, role in
                                                enumerate(("client", "forwarder", "authority"))},
                workload_fingerprint_sha256="trace", ttl_fingerprint_sha256="ttls",
                warm_up_queries=10, total_queries=20, timing_valid=True,
                timing_validation=dict(valid=True, reasons=[], mean_lag_seconds=0),
                observed_simulated_seconds=cfg["warm_up_seconds"] + cfg["duration_seconds"],
                average_latency_ms=10, average_wall_latency_ms=1,
                authoritative_queries=10, successful_reverse_queries=0, manager_queries=0,
                cluster_hits=0, wall_run_seconds=1)


class ComparisonTests(unittest.TestCase):
    def test_failed_formal_redns_reruns_both_and_only_publishes_complete_pair(self):
        calls = []
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            previous = path / "comparison.json"
            previous.write_text('{"previous": true}')

            def runner(cfg, model, cpu_pool):
                calls.append((model, cfg["time_scale"], cfg["duration_seconds"], tuple(cpu_pool)))
                self.assertEqual(json.loads(previous.read_text()), {"previous": True})
                # Pilots use one second. Fail only the first, longer formal ReDNS.
                if model == "ReDNS" and cfg["time_scale"] == 1 and cfg["duration_seconds"] == 3:
                    raise SimulationOverload({"reasons": ["forced sustained backlog"]})
                return fake_result(cfg, model, cpu_pool)

            cfg = config(time_scale=1, duration_seconds=3, warm_up_seconds=1,
                         timing={"probe_duration_seconds": 1, "probe_warm_up_seconds": 0})
            with redirect_stdout(io.StringIO()):
                manifest = run_comparison(cfg, path, runner=runner)
            self.assertEqual([scale for _, scale, duration, _ in calls if duration == 3], [1, 1, 2, 2])
            self.assertEqual(len({pool for *_, pool in calls}), 1)
            self.assertEqual(manifest["actual_time_scale"], 2)
            self.assertEqual(len(list(path.glob("run-*"))), 1)
            self.assertEqual(list(path.glob(".trial-*")), [])
            for model in ("baseline", "ReDNS"):
                saved = json.loads((path / manifest["results"][model]).read_text())
                self.assertEqual(saved["config"]["time_scale"], 2)

    def test_pilots_use_both_models_and_choose_one_shared_auto_scale(self):
        calls = []

        def runner(cfg, model, cpu_pool):
            calls.append((model, cfg["time_scale"]))
            result = fake_result(cfg, model, cpu_pool)
            if model == "ReDNS" and cfg["time_scale"] == 2:
                result.update(timing_valid=False, timing_validation={"reasons": ["backlog"]})
            return result

        with tempfile.TemporaryDirectory() as folder, redirect_stdout(io.StringIO()):
            manifest = run_comparison(config(time_scale="auto"), folder, runner=runner)
        self.assertEqual(calls[:4], [("baseline", 2), ("ReDNS", 2), ("baseline", 4), ("ReDNS", 4)])
        self.assertEqual(manifest["actual_time_scale"], 4)

    def test_input_mismatch_is_rejected_and_failure_preserves_previous_pair(self):
        cfg = config(time_scale=1, timing={"max_attempts": 1})
        rows = {model: fake_result(cfg, model, ()) for model in ("baseline", "ReDNS")}
        for field in ("workload_fingerprint_sha256", "ttl_fingerprint_sha256", "total_queries"):
            bad = deepcopy(rows)
            bad["ReDNS"][field] = "different"
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "inputs differ"):
                validate_pair(bad, cfg, ())
        with tempfile.TemporaryDirectory() as folder:
            previous = Path(folder) / "comparison.json"
            previous.write_text('{"previous": true}')

            def failed(*args):
                raise SimulationOverload({"reasons": ["forced overload"]})

            with redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "No new pair"):
                run_comparison(cfg, folder, runner=failed)
            self.assertEqual(json.loads(previous.read_text()), {"previous": True})

    def test_real_paired_processes_match_inputs_and_exercise_manager(self):
        cfg = config(time_scale=0.5, num_clients=6, num_domains=30,
                     forwarder_cluster_size=2, queries_per_second=60,
                     warm_up_seconds=1, duration_seconds=1,
                     timing={"max_mean_lag_seconds": 0.05, "max_overrun_seconds": 0.5,
                             "probe_duration_seconds": 1, "probe_warm_up_seconds": 1})
        with tempfile.TemporaryDirectory() as folder, redirect_stdout(io.StringIO()):
            manifest = run_comparison(cfg, folder)
            rows = manifest["models"]
            self.assertEqual(rows["baseline"]["actual_time_scale"], rows["ReDNS"]["actual_time_scale"])
            self.assertEqual(rows["baseline"]["workload_fingerprint_sha256"],
                             rows["ReDNS"]["workload_fingerprint_sha256"])
            self.assertGreater(rows["ReDNS"]["manager_queries"], 0)
            self.assertEqual(rows["baseline"]["manager_queries"], 0)
            for model in rows:
                result = json.loads((Path(folder) / manifest["results"][model]).read_text())
                self.assertTrue(result["timing_valid"])
                self.assertEqual(result["ipc_messages_sent"], result["ipc_messages_received"])
