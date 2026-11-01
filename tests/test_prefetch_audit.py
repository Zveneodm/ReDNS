"""Regressions found by auditing the first prefetch experiment."""

import unittest

from sim.config import validate_config
from src.prefetch import Forestall
from tests.helpers import ProtocolHarness


class PrefetchAuditTests(unittest.TestCase):
    def harness(self, **overrides):
        return ProtocolHarness("ReDNS-prefetch", **{
            "num_forwarders": 2, "forwarder_cluster_size": 2,
            "duration_seconds": 20, "client_cache_size": 0,
            "prefetch": {"report_seconds": 1, "advice_seconds": 5}, **overrides})

    def tick(self, harness, when):
        harness.clock.advance_to(when)
        harness.layers["forwarder"].prefetch_tick()
        harness.drain()

    def test_advice_uses_fixed_boundaries_despite_report_jitter(self):
        harness = self.harness()
        self.tick(harness, 5.002)
        manager = harness.layers["manager"].managers[0]
        self.assertEqual(manager.stats["prefetch_advice_runs"], 1)
        self.tick(harness, 10.001)
        self.assertEqual(manager.stats["prefetch_advice_runs"], 2)
        self.tick(harness, 15.0005)
        self.assertEqual(manager.stats["prefetch_advice_runs"], 3)

    def test_skipped_fetch_does_not_consume_unsuccessful_advice_budget(self):
        harness = self.harness(forwarder_cache_size=0,
                               prefetch={"report_seconds": 1, "advice_seconds": 1, "zeta": 0})
        manager_layer = harness.layers["manager"]
        tracker = manager_layer.prefetchers[0]
        for when in (0, 0.5, 0.9):
            tracker.observe(7, when, 2, 2)
        self.tick(harness, 1)
        self.tick(harness, 1.9)
        self.assertEqual(sum(node.stats["prefetch_skipped"]
                             for node in harness.layers["forwarder"].nodes), 1)
        tracker.advise(1.9)
        self.assertIn(7, tracker.tracking)
        self.assertEqual(tracker.tracking[7].failed_advice, 0)

    def test_completed_fetch_counts_as_an_advice(self):
        harness = self.harness(prefetch={"report_seconds": 1, "advice_seconds": 1})
        tracker = harness.layers["manager"].prefetchers[0]
        for when in (0, 0.5, 0.9):
            tracker.observe(7, when, 2, 2)
        self.tick(harness, 1)
        self.tick(harness, 1.9)
        self.assertTrue(tracker.tracking[7].advice_pending)
        self.assertEqual(tracker.tracking[7].last_advice, 1)

    def test_late_reply_is_merged_before_newer_completed_transactions(self):
        harness = self.harness()
        forwarder = harness.layers["forwarder"]
        harness.clock.advance_to(0.9)
        older_reply = forwarder._demand_started(0, 7, lambda answer: None)
        harness.clock.advance_to(0.95)
        newer_reply = forwarder._demand_started(1, 7, lambda answer: None)
        newer_reply((10, 10, 0))
        self.tick(harness, 1)
        tracker = harness.layers["manager"].prefetchers[0]
        self.assertNotIn(7, tracker.tracking)
        harness.clock.advance_to(1.2)
        older_reply((10, 10, 0))
        self.tick(harness, 2)
        self.assertEqual(tracker.tracking[7].intervals.count, 1)
        self.assertAlmostEqual(tracker.tracking[7].intervals.mean, 0.05)

    def test_tracker_rejects_silently_corrupting_out_of_order_input(self):
        tracker = Forestall(validate_config({})["prefetch"])
        tracker.observe(7, 2, 10, 10)
        with self.assertRaisesRegex(ValueError, "ordered"):
            tracker.observe(7, 1, 10, 10)


if __name__ == "__main__":
    unittest.main()
