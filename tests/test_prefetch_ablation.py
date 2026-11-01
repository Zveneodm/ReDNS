from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest

from lib.comparison import PREFETCH_ABLATION_MODELS, run_comparison, validate_pair
from lib.metrics import validate_results
from lib.realtime import RealtimeSimulation
from lib.analysis.prefetch import main as report_main
from src.models import BaselineForwarder
from src.records import Record
from tests.helpers import ProtocolHarness, config
from tests.test_timing import fake_result


class PrefetchAblationTests(unittest.TestCase):
    def test_same_prefetch_policy_with_and_without_cooperative_retrieval(self):
        for model in PREFETCH_ABLATION_MODELS:
            with self.subTest(model=model):
                harness = ProtocolHarness(model, client_cache_size=0, ttl_range=[2, 2],
                                          forwarder_cluster_size=2, duration_seconds=5,
                                          prefetch={"report_seconds": 1, "advice_seconds": 1,
                                                    "budget_ratio": 1})
                forwarder = harness.layers["forwarder"]
                for when in (0, 0.2, 0.8, 0.9):
                    harness.clock.advance_to(when)
                    harness.query(0, 7)
                forwarder.prefetch_load[0] = 20
                harness.clock.advance_to(1)
                forwarder.prefetch_tick()
                harness.drain()
                self.assertEqual(forwarder.prefetch_queue[0][1:3], (1, 7))
                harness.clock.advance_to(1.9)
                forwarder.prefetch_tick()
                harness.drain()
                self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 1)
                harness.clock.advance_to(2.1)
                self.assertEqual(harness.query(1, 7)[2], 1)
                answer = harness.query(0, 7)
                self.assertEqual(answer[2], 111 if model == "baseline-prefetch" else 4)
                self.assertEqual(harness.layers["authority"].authority.queries,
                                 2 if model == "baseline-prefetch" else 1)
                self.assertEqual(forwarder.nodes[1].stats["prefetch_records_used"], 1)
                if model == "baseline-prefetch":
                    self.assertIsInstance(forwarder.nodes[0], BaselineForwarder)
                    self.assertFalse(forwarder.cooperation_enabled)
                    self.assertFalse(harness.layers["manager"].managers[0].by_domain)
                    for field in ("peer_queries", "manager_queries", "cluster_queries",
                                  "records_transferred", "manager_updates_sent"):
                        self.assertEqual(forwarder.nodes[0].stats[field], 0)
                    with self.assertRaisesRegex(ValueError, "ReDNS forwarders"):
                        forwarder._serve_cluster(1, 99, 0, 7, True)

    def test_baseline_prefetch_does_not_use_client_copies_or_transfer_evictions(self):
        harness = ProtocolHarness("baseline-prefetch", forwarder_cluster_size=2,
                                  forwarder_cache_size=1, client_cache_size=2)
        harness.layers["client"]._select(2, True).insert_cache(Record(7, 100, 100))
        harness.drain()
        self.assertEqual(harness.query(0, 7)[2], 111)
        harness.query(0, 8)
        forwarder = harness.layers["forwarder"].nodes[0]
        self.assertEqual(forwarder.stats["peer_queries"], 0)
        self.assertEqual(forwarder.stats["records_transferred"], 0)
        self.assertEqual(harness.layers["client"].nodes[2].stats["directory_stores_sent"], 0)

    def test_ablation_publication_has_exact_pair_and_checks_manager_cpu(self):
        cfg = config(time_scale=1, duration_seconds=1,
                     timing={"probe_duration_seconds": 1, "probe_warm_up_seconds": 0})
        with tempfile.TemporaryDirectory() as folder, redirect_stdout(io.StringIO()):
            manifest = run_comparison(cfg, folder, runner=fake_result, prefetch_ablation=True)
            self.assertEqual(set(manifest["models"]), set(PREFETCH_ABLATION_MODELS))
            self.assertEqual(manifest["comparison_kind"], "prefetch_ablation")
            self.assertTrue(all((Path(folder) / path).exists() for path in manifest["results"].values()))
            with self.assertRaisesRegex(ValueError, "either"):
                run_comparison(cfg, folder, include_prefetch=True, prefetch_ablation=True)
        rows = {model: fake_result(cfg, model, ()) for model in PREFETCH_ABLATION_MODELS}
        for row in rows.values():
            row["workers"]["manager"] = {"cpu": 3, "role": "manager"}
        validate_pair(rows, cfg, ())
        bad = deepcopy(rows)
        bad["ReDNS-prefetch"]["workers"]["manager"]["cpu"] = -1
        with self.assertRaisesRegex(RuntimeError, "CPU binding"):
            validate_pair(bad, cfg, ())
        bad = deepcopy(rows)
        bad["ReDNS-prefetch"]["workload_fingerprint_sha256"] = "different"
        with self.assertRaisesRegex(RuntimeError, "inputs differ"):
            validate_pair(bad, cfg, ())

    def test_real_ablation_runs_prefetch_validates_results_and_writes_report(self):
        cfg = config(num_clients=12, num_forwarders=4, num_domains=12,
                     client_cache_size=0, forwarder_cache_size=12, forwarder_cluster_size=2,
                     workers={"client": 2, "forwarder": 2, "manager": 2},
                     warm_up_seconds=1, duration_seconds=4, queries_per_second=200,
                     time_scale=0.25, ttl_range=[2, 2],
                     prefetch={"report_seconds": 0.25, "advice_seconds": 0.5},
                     timing={"probe_warm_up_seconds": 0, "probe_duration_seconds": 1})
        with tempfile.TemporaryDirectory() as folder, redirect_stdout(io.StringIO()):
            manifest = run_comparison(cfg, folder, prefetch_ablation=True)
            for model, relative in manifest["results"].items():
                result = json.loads((Path(folder) / relative).read_text())
                validate_results(result)
                self.assertGreater(result["prefetch_authoritative_queries"], 0)
                self.assertTrue(result["prefetch_enabled"])
                self.assertEqual(result["cooperation_enabled"], model == "ReDNS-prefetch")
                if model == "baseline-prefetch":
                    for field in ("peer_queries", "cluster_queries", "records_transferred", "manager_queries"):
                        self.assertEqual(result["forwarder"][field], 0)
            output = Path(folder) / "report"
            report_main([folder, "--output-dir", str(output)])
            report = json.loads((output / "summary.json").read_text())
            self.assertEqual(set(report["models"]), set(PREFETCH_ABLATION_MODELS))
            gains = report["comparisons"]["ReDNS-prefetch_vs_baseline-prefetch"]
            reference = report["models"]["baseline-prefetch"]["average_latency_ms"]
            candidate = report["models"]["ReDNS-prefetch"]["average_latency_ms"]
            self.assertAlmostEqual(gains["average_latency_ms_reduction"], 1 - candidate / reference)
            self.assertTrue((output / "report.md").exists())


if __name__ == "__main__":
    unittest.main()
