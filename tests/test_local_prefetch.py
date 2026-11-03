"""Local forecast isolation, pinned placement and demand-funded shared quotas."""

import unittest

from src.auxiliary.prefetch_budget import ClusterPrefetchBudget
from src.records import Record
from tests.helpers import ProtocolHarness
from tests.test_parallel import ShardedHarness


class PinnedBudgetTests(unittest.TestCase):
    def test_same_domain_can_be_prefetched_by_two_forwarders_and_busy_owner_waits(self):
        budget = ClusterPrefetchBudget(1, (0, 1), 1, 2)
        budget.fund((0, 3))
        budget.offer(0, [(7, 0, 10, 0, 1), (8, 0, 10, 0, 1)], 0, target=0)
        budget.offer(1, [(7, 1, 10, 0, 1)], 0, target=1)
        first, target = budget.take(1, True)
        self.assertEqual((first.domain, target), (7, 0))
        second, target = budget.take(1, True)
        self.assertEqual((second.domain, target), (7, 1))
        self.assertIn((0, 8), budget.pending)
        self.assertIsNone(budget.take(1, True))
        budget.finish(first.key, True)
        third, target = budget.take(1, True)
        self.assertEqual((third.domain, target), (8, 0))
        budget.finish(second.key, True)
        budget.finish(third.key, True)
        self.assertEqual(budget.available(True), 0)

    def test_replacing_one_forwarder_advice_preserves_other_forwarders(self):
        budget = ClusterPrefetchBudget(1, (0, 1), 1, 2)
        for target in (0, 1):
            budget.offer(target, [(7, 0, 10, 0, 1)], 0, target=target)
        budget.offer(0, [], 1, target=0)
        self.assertEqual(set(budget.pending), {(1, 7)})
        budget.fund((0, 1))
        self.assertEqual(budget.take(1, True)[1], 1)


class LocalPrefetchTests(unittest.TestCase):
    def harness(self, **overrides):
        return ProtocolHarness("baseline-prefetch", **{
            "num_clients": 4, "num_forwarders": 2, "forwarder_cluster_size": 2,
            "client_cache_size": 0, "forwarder_cache_size": 8, "ttl_range": [2, 2],
            "duration_seconds": 6,
            "prefetch": {"report_seconds": 1, "advice_seconds": 1, "budget_ratio": 1},
            **overrides,
        })

    def tick(self, harness, when):
        harness.clock.advance_to(when)
        for layer in harness.layers.values():
            layer.prefetch_tick()
        harness.drain()

    def prime(self, harness, client=0):
        for when in (0, 0.2, 0.8, 0.9):
            harness.clock.advance_to(when)
            harness.query(client, 7)

    def test_local_demand_prefetches_to_same_forwarder_even_when_another_is_idle(self):
        harness = self.harness()
        self.prime(harness)
        forwarder, manager = harness.layers["forwarder"], harness.layers["manager"]
        forwarder.prefetch_load[0] = 10000
        self.tick(harness, 1)
        self.assertEqual(forwarder.prefetch_queue[0][1:3], (0, 7))
        self.assertNotIn(7, forwarder.local_prefetchers[1].tracking)
        self.assertEqual(manager.prefetchers, {})
        self.assertEqual(manager.managers[0].stats["prefetch_observations_received"], 0)
        self.assertEqual(manager.managers[0].stats["prefetch_advice_runs"], 0)
        self.tick(harness, 1.9)
        harness.clock.advance_to(2.1)
        self.assertEqual(harness.query(0, 7)[2], 1)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)
        self.assertEqual(forwarder.nodes[0].stats["prefetch_records_used"], 1)
        self.assertEqual(forwarder.nodes[1].stats["prefetch_stored"], 0)
        self.assertFalse(forwarder.local_prefetch_grants)

    def test_intervals_expiration_and_same_domain_prefetches_are_independent(self):
        harness = self.harness()
        for when, client in ((0, 0), (0, 1), (0.2, 0), (0.6, 1), (0.8, 0), (0.9, 0)):
            harness.clock.advance_to(when)
            harness.query(client, 7)
        self.tick(harness, 1)
        forwarder = harness.layers["forwarder"]
        first, second = (forwarder.local_prefetchers[node].tracking[7] for node in (0, 1))
        self.assertAlmostEqual(first.intervals.mean, 0.3)
        self.assertAlmostEqual(second.intervals.mean, 0.6)
        self.assertEqual((first.intervals.count, second.intervals.count), (3, 1))
        self.assertEqual({row[1] for row in forwarder.prefetch_queue}, {0, 1})
        self.tick(harness, 1.9)
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 2)
        first.expires_at = 100
        self.assertLess(second.expires_at, 100)

    def test_late_replies_keep_local_observations_ordered(self):
        harness = self.harness()
        forwarder = harness.layers["forwarder"]
        first = forwarder._demand_started(0, 7, lambda answer: None, measure=True)
        harness.clock.advance_to(0.1)
        second = forwarder._demand_started(0, 7, lambda answer: None, measure=True)
        second((2, 2, 0))
        self.tick(harness, 1)
        self.assertNotIn(7, forwarder.local_prefetchers[0].tracking)
        first((2, 2, 0))
        self.tick(harness, 2)
        entry = forwarder.local_prefetchers[0].tracking[7]
        self.assertEqual(entry.last_arrival, 0.1)
        self.assertAlmostEqual(entry.intervals.mean, 0.1)

    def test_shared_ratio_keeps_fractional_credit_across_forwarders_and_manager_shards(self):
        harness = ShardedHarness("baseline-prefetch", num_clients=20, num_forwarders=20,
                                 forwarder_cluster_size=20, client_cache_size=0,
                                 workers={"client": 2, "forwarder": 2, "manager": 2},
                                 prefetch={"budget_ratio": 0.15})
        for client in range(20):
            harness.query(client, 7)
        self.tick(harness, 1)
        managers = [layer for layer in harness.layers.values() if layer.role == "manager"]
        budget = next(layer.prefetch_budgets[0] for layer in managers if 0 in layer.prefetch_budgets)
        self.assertEqual(budget.demand[True], 20)
        self.assertEqual(budget.allowance(True), 3)
        self.assertEqual(sum(layer.managers[0].stats["prefetch_reports_received"] for layer in managers), 20)
        self.assertTrue(all(not layer.prefetchers for layer in managers))

    def test_stale_grant_refunds_credit_without_authority_query(self):
        harness = self.harness()
        self.prime(harness)
        forwarder, manager = harness.layers["forwarder"], harness.layers["manager"]
        harness.clock.advance_to(1)
        forwarder.prefetch_tick()
        while forwarder.outbox:
            role, message = forwarder.outbox.popleft()
            harness.layers[role]._message(message)
        self.assertEqual(manager.outbox[0][1][0], "prefetch_local_grant")
        forwarder.local_prefetchers[0].refreshed(7, 100)
        harness.drain()
        self.assertEqual(manager.managers[0].stats["prefetch_budget_refunded"], 1)
        self.assertEqual(forwarder.nodes[0].stats["prefetch_skipped"], 1)
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 0)
        self.assertFalse(forwarder.local_prefetch_grants)

    def test_fresh_foreground_fill_cancels_a_queued_local_prefetch(self):
        harness = self.harness()
        self.prime(harness)
        self.tick(harness, 1)
        forwarder = harness.layers["forwarder"]
        forwarder._select(0, True).insert_cache(Record(7, 100, 100))
        self.tick(harness, 1.9)
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 0)
        self.assertEqual(harness.layers["manager"].managers[0].stats["prefetch_budget_refunded"], 1)
        self.assertFalse(forwarder.local_prefetch_grants)

    def test_zero_ratio_and_zero_capacity_do_not_spend_quota(self):
        for options in ({"prefetch": {"budget_ratio": 0, "advice_seconds": 1}},
                        {"forwarder_cache_size": 0}):
            with self.subTest(options=options):
                harness = self.harness(**options)
                self.prime(harness)
                self.tick(harness, 1)
                self.tick(harness, 1.9)
                self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 0)
                budget = harness.layers["manager"].prefetch_budgets[0]
                self.assertEqual(budget.reserved[True] - budget.refunded[True], 0)


if __name__ == "__main__":
    unittest.main()
