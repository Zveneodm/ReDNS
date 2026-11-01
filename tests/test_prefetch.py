from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest

from sim.comparison import run_comparison, validate_pair
from sim.config import validate_config
from sim.realtime import RealtimeSimulation
from src.prefetch import Forestall
from src.records import Record
from tests.helpers import ProtocolHarness, config
from tests.test_parallel import ShardedHarness
from tests.test_timing import fake_result


class ForecastTests(unittest.TestCase):
    def tracker(self, **overrides):
        return Forestall(validate_config({"prefetch": overrides})["prefetch"])

    def test_singletons_fresh_records_and_zero_ttl_are_not_advised(self):
        tracker = self.tracker()
        tracker.observe(0, 0, 1, 1)
        tracker.observe(1, 0, 100, 100)
        tracker.observe(1, 1, 100, 100)
        tracker.observe(2, 0, 0, 0)
        tracker.observe(2, 1, 1, 0)
        self.assertEqual(tracker.advise(1), [])

    def test_welford_prediction_deadline_and_successful_refresh(self):
        tracker = self.tracker(advice_seconds=3)
        for arrival in (0, 1, 3):
            tracker.observe(7, arrival, 4, 4)
        entry = tracker.tracking[7]
        self.assertEqual(entry.intervals.count, 2)
        self.assertEqual(entry.intervals.mean, 1.5)
        self.assertEqual(entry.intervals.m2, 0.5)
        self.assertEqual(tracker.advise(3), [(4, 7)])
        tracker.refreshed(7, 20)
        self.assertEqual(tracker.advise(3), [])

    def test_failed_advice_counted_once_and_inactive_entries_removed(self):
        tracker = self.tracker(zeta=0, history_seconds=3)
        tracker.observe(7, 0, 1, 1)
        tracker.observe(7, 1, 2, 1)
        tracker.mark_advised(7, 1)
        self.assertEqual(tracker.advise(2), [])
        self.assertNotIn(7, tracker.tracking)
        tracker.observe(8, 0, 1, 1)
        tracker.advise(4)
        self.assertNotIn(8, tracker.tracking)

    def test_invalid_configuration_is_rejected_and_defaults_are_copied(self):
        for value in (None, {"enabled": True}, {"alpha": 0}, {"beta": -1},
                      {"gamma": float("nan")}, {"zeta": -1}, {"epsilon": True},
                      {"max_per_forwarder": 0}, {"report_seconds": 0}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_config({"prefetch": value})
        cfg = validate_config({})
        cfg["prefetch"]["alpha"] = 99
        self.assertEqual(validate_config({})["prefetch"]["alpha"], 1)


class PrefetchProtocolTests(unittest.TestCase):
    def harness(self, **overrides):
        return ProtocolHarness("ReDNS-prefetch", **{
            "forwarder_cluster_size": 2, "client_cache_size": 0,
            "ttl_range": [2, 2], "duration_seconds": 5,
            "prefetch": {"report_seconds": 1, "advice_seconds": 1, "budget_ratio": 1}, **overrides})

    def tick(self, harness, when):
        harness.clock.advance_to(when)
        harness.layers["forwarder"].prefetch_tick()
        harness.drain()

    def test_refresh_is_forced_and_assigned_to_idle_forwarder_then_discovered(self):
        harness = self.harness()
        harness.query(0, 7)
        harness.clock.advance_to(0.2)
        harness.query(0, 7)
        harness.clock.advance_to(0.8)
        harness.query(0, 7)
        harness.clock.advance_to(0.9)
        harness.query(0, 7)
        fw = harness.layers["forwarder"]
        fw.prefetch_load[0] = 20
        self.tick(harness, 1)
        self.assertEqual(len(fw.prefetch_queue), 1)
        self.assertEqual(fw.prefetch_queue[0][1:3], (1, 7))
        self.tick(harness, 1.9)
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 1)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)
        harness.clock.advance_to(2.1)
        self.assertEqual(harness.query(0, 7)[2], 4)
        self.assertEqual(fw.nodes[1].stats["prefetch_records_used"], 1)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)

    def test_manager_orders_query_candidates_by_reported_load(self):
        harness = self.harness(num_forwarders=3, num_clients=6, forwarder_cluster_size=3)
        fw = harness.layers["forwarder"]
        for member in (1, 2):
            fw._select(member, True).insert_cache(Record(7, 100, 100))
        fw.prefetch_load[1] = 100
        harness.drain()
        self.tick(harness, 1)
        manager = harness.layers["manager"].managers[0]
        self.assertEqual([row[0] for row in manager.lookup(0, 7)], [2, 1])
        harness.query(0, 7)
        self.assertEqual(fw.nodes[2].stats["cluster_queries_received"], 1)
        self.assertEqual(fw.nodes[1].stats["cluster_queries_received"], 0)

    def test_singleton_cluster_prefetches_without_neighbor_lookups(self):
        harness = self.harness(num_forwarders=1, num_clients=2, forwarder_cluster_size=1)
        for when in (0, 0.2, 0.8, 0.9):
            harness.clock.advance_to(when)
            harness.query(0, 7)
        self.tick(harness, 1)
        self.tick(harness, 1.9)
        harness.clock.advance_to(2.1)
        self.assertEqual(harness.query(1, 7)[2], 1)
        node = harness.layers["forwarder"].nodes[0]
        self.assertEqual(node.stats["prefetch_completed"], 1)
        self.assertEqual(node.stats["manager_queries"], 0)
        self.assertEqual(node.stats["cluster_queries"], 0)

    def test_no_prefetch_after_horizon_and_zero_capacity_is_skipped(self):
        harness = self.harness(forwarder_cache_size=0)
        fw = harness.layers["forwarder"]
        fw.handle_message(("prefetch_task", 1, 7, 1, True))
        self.tick(harness, 1)
        self.assertEqual(fw.nodes[1].stats["prefetch_skipped"], 1)
        fw.handle_message(("prefetch_task", 1, 7, 5, True))
        self.tick(harness, 5)
        self.assertEqual(fw.nodes[1].stats["prefetch_skipped"], 2)
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 0)

    def test_domain_shards_merge_real_observations_without_crossing_clusters(self):
        harness = ShardedHarness("ReDNS-prefetch", num_clients=8, num_forwarders=4,
                                 forwarder_cluster_size=2, client_cache_size=0,
                                 workers={"forwarder": 2, "manager": 2},
                                 prefetch={"report_seconds": 1, "advice_seconds": 1})
        harness.query(0, 7)
        harness.clock.advance_to(0.2)
        harness.query(1, 7)
        harness.clock.advance_to(1)
        for layer in harness.layers.values():
            layer.prefetch_tick()
        harness.drain()
        name = harness.layout.owner("manager", 0, 7)
        tracker = harness.layers[name].prefetchers[0]
        self.assertEqual(tracker.tracking[7].intervals.count, 1)
        self.assertAlmostEqual(tracker.tracking[7].intervals.mean, 0.2)
        for layer in harness.layers.values():
            if layer.role == "manager":
                self.assertNotIn(7, layer.prefetchers[1].tracking)


class PrefetchRuntimeTests(unittest.TestCase):
    def test_real_sharded_prefetch_drains_and_preserves_foreground_accounting(self):
        cfg = config(num_clients=12, num_forwarders=4, num_domains=12,
                     client_cache_size=0, forwarder_cache_size=12, forwarder_cluster_size=2,
                     workers={"client": 2, "forwarder": 2, "manager": 2},
                     warm_up_seconds=1, duration_seconds=5, queries_per_second=200,
                     time_scale=0.25, ttl_range=[2, 2],
                     prefetch={"report_seconds": 0.25, "advice_seconds": 0.5})
        with redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(cfg, "ReDNS-prefetch").run()
        self.assertGreater(result["prefetch_authoritative_queries"], 0)
        self.assertGreater(result["manager"]["prefetch_observations_received"], 0)
        self.assertEqual(result["client"]["queries"], result["total_queries"])
        self.assertEqual(result["ipc_messages_sent"], result["ipc_messages_received"])

    def test_three_way_publication_rejects_different_prefetch_inputs(self):
        cfg = config(time_scale=1, duration_seconds=1,
                     timing={"probe_duration_seconds": 1, "probe_warm_up_seconds": 0})
        with tempfile.TemporaryDirectory() as folder, redirect_stdout(io.StringIO()):
            manifest = run_comparison(cfg, folder, runner=fake_result, include_prefetch=True)
            self.assertEqual(set(manifest["models"]), {"baseline", "ReDNS", "ReDNS-prefetch"})
            for path in manifest["results"].values():
                self.assertTrue((Path(folder) / path).exists())
        rows = {model: fake_result(cfg, model, ()) for model in manifest["models"]}
        rows["ReDNS-prefetch"]["ttl_fingerprint_sha256"] = "different"
        with self.assertRaisesRegex(RuntimeError, "inputs differ"):
            validate_pair(rows, cfg, ())


if __name__ == "__main__":
    unittest.main()
