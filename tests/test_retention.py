from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from sim.observe_retention import read_profile, summarize_groups
from sim.realtime import RealtimeSimulation
from sim.retention import INDEX, RetentionObserver
from src.records import Record
from tests.helpers import ProtocolHarness, config


class RetentionTests(unittest.TestCase):
    def attach(self, harness, directory):
        return {role: RetentionObserver(layer, range(len(layer.nodes)), directory)
                for role, layer in harness.layers.items() if role in ("client", "forwarder")}

    def test_observer_preserves_answers_cache_order_and_model_counters(self):
        for model in ("baseline", "ReDNS"):
            for policy in ("lru", "random"):
                plain = ProtocolHarness(model, replacement_policy=policy, forwarder_cluster_size=2)
                watched = ProtocolHarness(model, replacement_policy=policy, forwarder_cluster_size=2)
                with tempfile.TemporaryDirectory() as directory:
                    observers = self.attach(watched, directory)
                    for when, client, domain in ((0, 0, 1), (1, 2, 2), (2, 0, 3),
                                                 (3, 2, 1), (4, 1, 4), (5, 3, 5), (6, 1, 4)):
                        for harness in (plain, watched):
                            harness.clock.advance_to(when)
                        self.assertEqual(plain.query(client, domain), watched.query(client, domain))
                    for role in ("client", "forwarder"):
                        for first, second in zip(plain.layers[role].nodes, watched.layers[role].nodes):
                            self.assertEqual(list(first.cache), list(second.cache))
                            self.assertEqual(first.metrics(), second.metrics())
                    for observer in observers.values():
                        observer.snapshot()

    def test_warm_boundary_capacity_exit_and_ttl_exit_have_correct_live_times(self):
        harness = ProtocolHarness(warm_up_seconds=2, client_cache_size=1)
        with tempfile.TemporaryDirectory() as directory:
            observer = RetentionObserver(harness.layers["client"], [0], directory)
            node = harness.layers["client"]._select(0, True)
            node.insert_cache(Record(1, 10, 10))
            harness.clock.advance_to(4)
            node.insert_cache(Record(2, 6, 6))
            harness.clock.advance_to(7)
            node.check_cache(2)
            harness.clock.advance_to(8)
            observer.snapshot()
            first, second = observer.rows[0][1], observer.rows[0][2]
            self.assertEqual(first[INDEX["live_seconds"]], 2)
            self.assertEqual(first[INDEX["capacity_evictions"]], 1)
            self.assertEqual(first[INDEX["warm_entries"]], 1)
            self.assertEqual(second[INDEX["live_seconds"]], 2)
            self.assertEqual(second[INDEX["ttl_expirations"]], 1)

    def test_duplicate_advertisement_does_not_create_another_episode(self):
        harness = ProtocolHarness()
        with tempfile.TemporaryDirectory() as directory:
            observer = RetentionObserver(harness.layers["client"], [0], directory)
            node = harness.layers["client"]._select(0, True)
            node.insert_cache(Record(1, 10, 100))
            node.receive_transfer(Record(1, 100, 100))
            harness.clock.advance_to(5)
            totals = observer.snapshot()["totals"]
            self.assertEqual(totals["episodes"], 1)
            self.assertEqual(totals["live_at_end"], 1)
            self.assertEqual(totals["live_seconds"], 5)

    def test_lazy_expired_entry_stops_live_time_at_deadline(self):
        harness = ProtocolHarness()
        with tempfile.TemporaryDirectory() as directory:
            observer = RetentionObserver(harness.layers["client"], [0], directory)
            node = harness.layers["client"]._select(0, True)
            node.insert_cache(Record(1, 5, 5))
            harness.clock.advance_to(10)
            totals = observer.snapshot()["totals"]
            self.assertEqual(totals["live_seconds"], 5)
            self.assertEqual(totals["ttl_expirations"], 1)
            self.assertEqual(totals["expired_pending_at_end"], 1)
            self.assertIn(1, node.cache)

    def test_record_expired_before_measurement_has_no_live_episode(self):
        harness = ProtocolHarness(warm_up_seconds=10)
        with tempfile.TemporaryDirectory() as directory:
            observer = RetentionObserver(harness.layers["client"], [0], directory)
            harness.layers["client"]._select(0, True).insert_cache(Record(1, 5, 5))
            harness.clock.advance_to(20)
            self.assertEqual(observer.snapshot()["totals"]["episodes"], 0)

    def test_same_domain_replacement_is_separate_from_capacity_eviction(self):
        harness = ProtocolHarness()
        with tempfile.TemporaryDirectory() as directory:
            observer = RetentionObserver(harness.layers["client"], [0], directory)
            node = harness.layers["client"]._select(0, True)
            node.insert_cache(Record(1, 10, 10))
            harness.clock.advance_to(1)
            node.insert_cache(Record(1, 10, 10))
            harness.clock.advance_to(5)
            totals = observer.snapshot()["totals"]
            self.assertEqual(totals["episodes"], 2)
            self.assertEqual(totals["replacements"], 1)
            self.assertEqual(totals["capacity_evictions"], 0)
            self.assertEqual(totals["live_seconds"], 5)

    def test_normal_queries_and_peer_queries_are_counted_separately(self):
        harness = ProtocolHarness()
        with tempfile.TemporaryDirectory() as directory:
            observers = self.attach(harness, directory)
            harness.layers["client"]._select(0, True).insert_cache(Record(7, 100, 100))
            harness.drain()
            harness.clock.advance_to(1)
            self.assertEqual(harness.query(2, 7)[2], 2)
            client = observers["client"]
            self.assertEqual(client.rows[0][7][INDEX["queries"]], 0)
            self.assertEqual(client.rows[0][7][INDEX["peer_hits"]], 1)
            self.assertEqual(client.rows[2][7][INDEX["queries"]], 1)
            self.assertEqual(observers["forwarder"].rows[0][7][INDEX["queries"]], 1)

    def test_compressed_profile_roundtrip_and_scope_specific_ranking(self):
        harness = ProtocolHarness()
        with tempfile.TemporaryDirectory() as directory:
            observers = self.attach(harness, directory)
            harness.query(0, 7)
            harness.clock.advance_to(1)
            harness.query(2, 7)
            harness.clock.advance_to(2)
            path = observers["forwarder"].snapshot()["path"]
            profile, rows = read_profile(path)
            groups = summarize_groups(profile, rows, {0: {7: 2}, 1: {8: 100}})["groups"]
            self.assertEqual(groups["top_1"]["pairs"], 2)
            self.assertEqual(groups["top_1"]["demand_queries"], 102)
            self.assertEqual(groups["top_1"]["pairs_ever_cached"], 1)

    def test_spawned_observation_matches_original_query_counters(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(config(duration_seconds=1, forwarder_cluster_size=2), "ReDNS", observation={
                "output_dir": directory, "sample_clients": 4,
            }).run()
            for role in ("client", "forwarder"):
                observed = result[f"{role}_retention"]
                self.assertEqual(observed["totals"]["queries"], result[role]["queries"])
                self.assertEqual(observed["totals"]["capacity_evictions"], result[role]["lru_evictions"])
                self.assertTrue(Path(observed["path"]).exists())

    def test_empty_observation_sample_has_zero_groups(self):
        groups = summarize_groups({"start": 0, "end": 10}, {}, {})["groups"]
        self.assertEqual(set(groups), {"top_1", "top_10", "top_100", "top_1000", "outside_top_1000"})
        self.assertTrue(all(row["pairs"] == 0 for row in groups.values()))

    def test_observation_cli_runs_both_models_and_separate_analysis(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "config.json"
            output = Path(directory) / "observation"
            source.write_text(json.dumps(config(queries_per_second=20)), encoding="utf-8")
            result = subprocess.run([
                sys.executable, "-m", "sim.observe_retention", "--config", str(source),
                "--output-dir", str(output), "--duration-seconds", "1", "--warm-up-seconds", "0",
                "--time-scale", "0.01", "--sample-clients", "0",
            ], cwd=root, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(set(summary["runs"]), {"baseline", "ReDNS"})
            self.assertTrue((output / "report.md").exists())
            self.assertEqual(summary["runs"]["baseline"]["total_queries"],
                             summary["runs"]["ReDNS"]["total_queries"])


if __name__ == "__main__":
    unittest.main()
