"""Demand-funded cluster budgets, shard ownership and dispatch concurrency."""

from contextlib import redirect_stdout
from fractions import Fraction
import io
import unittest

from lib.config import DEFAULT_CONFIG, validate_config
from lib.realtime import RealtimeSimulation
from src.auxiliary.prefetch_budget import ClusterPrefetchBudget
from tests.helpers import ProtocolHarness, config
from tests.test_parallel import ShardedHarness


class BudgetTests(unittest.TestCase):
    def budget(self, ratio=0.15, *, members=(0,), max_inflight=8):
        budget = ClusterPrefetchBudget(ratio, members, max_inflight, max(members) + 1)
        budget.loads = dict.fromkeys(members, 0)
        return budget

    def offer(self, budget, count=100, *, shard=0, valid_until=10):
        budget.offer(shard, [(domain, 0, valid_until, 0, 1) for domain in range(count)], 0)

    def complete_available(self, budget, now=1, measure=True):
        completed = 0
        while (task := budget.take(now, measure)) is not None:
            budget.finish(task[0].domain, True)
            completed += 1
        return completed

    def test_decimal_credit_is_exact_and_fractional_requests_accumulate(self):
        budget = self.budget()
        self.offer(budget)
        for _ in range(19):
            budget.fund((0, 1))
        self.assertEqual(budget.available(True), 2)
        budget.fund((0, 1))
        self.assertEqual(budget.available(True), 3)
        self.assertEqual(self.complete_available(budget), 3)

    def test_zero_and_one_ratios_and_monotonic_candidate_admission(self):
        counts = []
        for ratio in (0, 0.05, 0.15, 0.30, 1):
            budget = self.budget(ratio)
            budget.fund((0, 100))
            self.offer(budget)
            counts.append(self.complete_available(budget))
        self.assertEqual(counts, [0, 5, 15, 30, 100])

    def test_warm_credits_cannot_pay_for_formal_tasks(self):
        budget = self.budget()
        budget.fund((100, 1))
        self.offer(budget)
        self.assertEqual(budget.available(False), 15)
        self.assertIsNone(budget.take(1, True))
        budget.fund((0, 19))
        self.assertEqual(self.complete_available(budget), 3)
        self.assertEqual(budget.available(False), 15)

    def test_refund_releases_credit_and_concurrency(self):
        budget = self.budget(0.1, max_inflight=1)
        budget.fund((0, 10))
        self.offer(budget)
        candidate, _ = budget.take(1, True)
        self.assertIsNone(budget.take(1, True))
        budget.finish(candidate.domain, False)
        self.assertEqual(budget.available(True), 1)
        self.assertEqual(self.complete_available(budget), 1)
        self.assertEqual(budget.reserved[True] - budget.refunded[True], 1)

    def test_candidates_wait_for_credit_and_expired_advice_is_not_dispatched(self):
        budget = self.budget(1)
        self.offer(budget, 2, valid_until=3)
        self.assertIsNone(budget.take(1, True))
        self.assertEqual(len(budget.pending), 2)
        budget.fund((0, 1))
        self.assertEqual(self.complete_available(budget, now=2), 1)
        budget.fund((0, 1))
        self.assertIsNone(budget.take(4, True))
        self.assertEqual(budget.available(True), 1)

    def test_snapshot_replaces_only_its_domain_shard(self):
        budget = self.budget(1)
        budget.offer(0, [(0, 0, 10, 0, 1)], 0)
        budget.offer(1, [(1, 0, 10, 0, 1)], 0)
        budget.offer(0, [], 1)
        budget.fund((0, 2))
        self.assertEqual(self.complete_available(budget), 1)
        self.assertEqual(budget.available(True), 1)

    def test_equal_deadlines_prioritize_reuse_and_dispatch_to_lower_load(self):
        budget = self.budget(1, members=(0, 1), max_inflight=1)
        budget.loads = {0: 100, 1: 0}
        budget.fund((0, 20))
        budget.offer(0, [(0, 1, 10, 0, 4), (100, 1, 10, 0, 0.4)], 0)
        candidate, target = budget.take(1, True)
        self.assertEqual((candidate.domain, target), (100, 1))
        budget.finish(candidate.domain, True)
        self.assertEqual(budget.take(1, True)[1], 1)

    def test_inflight_cap_is_not_a_per_round_dispatch_quota(self):
        budget = self.budget(1, max_inflight=2)
        budget.fund((0, 30))
        self.offer(budget, 30)
        one, _ = budget.take(1, True)
        two, _ = budget.take(1, True)
        self.assertIsNone(budget.take(1, True))
        budget.finish(one.domain, True)
        budget.finish(two.domain, True)
        self.assertEqual(self.complete_available(budget), 28)


class BudgetConfigTests(unittest.TestCase):
    def test_ratio_bounds_and_concurrency_validation(self):
        for value in (-0.01, 1.01, True, False, None, "0.15", float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_config({"prefetch": {"budget_ratio": value}})
        for ratio in (0, 0.15, 1):
            self.assertEqual(validate_config({"prefetch": {"budget_ratio": ratio}})
                             ["prefetch"]["budget_ratio"], ratio)
        for value in (0, -1, True, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_config({"prefetch": {"max_inflight_per_forwarder": value}})

    def test_legacy_limit_migrates_to_concurrency_without_mutating_input(self):
        supplied = {"prefetch": {"max_per_forwarder": 3}}
        cfg = validate_config(supplied)
        self.assertEqual(cfg["prefetch"]["max_inflight_per_forwarder"], 3)
        self.assertNotIn("max_per_forwarder", cfg["prefetch"])
        self.assertEqual(supplied, {"prefetch": {"max_per_forwarder": 3}})
        self.assertEqual(DEFAULT_CONFIG["prefetch"]["max_inflight_per_forwarder"], 8)
        with self.assertRaisesRegex(ValueError, "conflict"):
            validate_config({"prefetch": {"max_per_forwarder": 3, "max_inflight_per_forwarder": 4}})


class BudgetProtocolTests(unittest.TestCase):
    def tick(self, harness, when, passes=1):
        harness.clock.advance_to(when)
        for _ in range(passes):
            for layer in harness.layers.values():
                layer.prefetch_tick()
            harness.drain()

    def test_manager_shards_fund_demand_once_and_exclude_helper_queries(self):
        harness = ShardedHarness("ReDNS-prefetch", num_clients=8, num_forwarders=4,
                                 forwarder_cluster_size=4, client_cache_size=0,
                                 forwarder_cache_size=32, workers={"forwarder": 2, "manager": 2},
                                 prefetch={"budget_ratio": 0.15, "report_seconds": 1})
        for index in range(100):
            harness.query(index % 8, index % 3)
        self.tick(harness, 1)
        managers = [layer for layer in harness.layers.values() if layer.role == "manager"]
        totals = [layer.managers[0].stats for layer in managers]
        self.assertEqual(sum(row["prefetch_budget_demand"] for row in totals), 100)
        self.assertEqual(sum(row["prefetch_budget_allowance"] for row in totals), 15)
        forwarders = [node for layer in harness.layers.values() if layer.role == "forwarder"
                      for node in layer.nodes if node is not None]
        self.assertGreater(sum(node.stats["cluster_queries_received"] for node in forwarders), 0)
        self.assertEqual(sum(node.stats["prefetch_budget_demand_reported"] for node in forwarders), 100)

    def test_cluster_budgets_do_not_borrow_from_each_other(self):
        harness = ShardedHarness("ReDNS-prefetch", num_clients=4, num_forwarders=4,
                                 forwarder_cluster_size=2, client_cache_size=0,
                                 workers={"manager": 2}, prefetch={"budget_ratio": 0.15})
        for _ in range(20):
            harness.query(0, 7)
        self.tick(harness, 1)
        budgets = {cluster: budget for layer in harness.layers.values()
                   for cluster, budget in layer.prefetch_budgets.items()}
        self.assertEqual(budgets[0].allowance(True), 3)
        self.assertEqual(budgets[1].allowance(True), 0)

    def test_partial_report_cohort_still_credits_all_received_demand(self):
        harness = ShardedHarness("ReDNS-prefetch", num_clients=4, num_forwarders=4,
                                 forwarder_cluster_size=4, client_cache_size=0,
                                 workers={"forwarder": 2, "manager": 2},
                                 prefetch={"budget_ratio": 1})
        harness.query(0, 7)
        harness.clock.advance_to(1)
        # Only half the forwarder owners get their timer turn, as can happen
        # just before the runtime takes its final distributed snapshot.
        harness.owner("forwarder", 0).prefetch_tick()
        harness.drain()
        manager = harness.layers[harness.layout.name("manager", 0)]
        self.assertTrue(manager.prefetch_reports)
        self.assertEqual(manager.prefetch_budgets[0].demand[True], 1)
        self.assertEqual(manager.managers[0].stats["prefetch_budget_demand"], 1)

    def test_stale_dispatch_is_refunded_before_contacting_a_forwarder(self):
        harness = ProtocolHarness("ReDNS-prefetch", client_cache_size=0,
                                  forwarder_cluster_size=2, ttl_range=[2, 2],
                                  prefetch={"budget_ratio": 1, "advice_seconds": 1})
        for when in (0, 0.2, 0.8, 0.9):
            harness.clock.advance_to(when)
            harness.query(0, 7)
        harness.clock.advance_to(1)
        forwarder, manager = harness.layers["forwarder"], harness.layers["manager"]
        forwarder.prefetch_tick()
        while forwarder.outbox:
            role, message = forwarder.outbox.popleft()
            harness.layers[role]._message(message)
        # First process the candidate batch, but hold its dispatch grant.
        role, batch = manager.outbox.popleft()
        self.assertEqual(batch[0], "prefetch_candidates")
        manager._message(batch)
        self.assertEqual(manager.outbox[0][1][0], "prefetch_dispatch")
        manager.prefetchers[0].refreshed(7, 100)
        harness.drain()
        stats = manager.managers[0].stats
        self.assertEqual(stats["prefetch_budget_reserved"], 1)
        self.assertEqual(stats["prefetch_budget_refunded"], 1)
        self.assertEqual(stats["prefetch_tasks_sent"], 0)

    def test_zero_ratio_generates_no_tasks_even_with_eligible_advice(self):
        harness = ProtocolHarness("ReDNS-prefetch", client_cache_size=0,
                                  forwarder_cluster_size=2, ttl_range=[2, 2],
                                  prefetch={"budget_ratio": 0, "advice_seconds": 1})
        for when in (0, 0.2, 0.8, 0.9):
            harness.clock.advance_to(when)
            harness.query(0, 7)
        self.tick(harness, 1)
        manager = harness.layers["manager"].managers[0]
        self.assertGreater(manager.stats["prefetch_advised"], 0)
        self.assertEqual(manager.stats["prefetch_tasks_sent"], 0)
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 0)

    def test_one_forwarder_can_complete_more_than_eight_tasks_in_one_round(self):
        harness = ProtocolHarness("ReDNS-prefetch", num_clients=1, num_forwarders=1,
                                  client_cache_size=0, forwarder_cache_size=20,
                                  ttl_range=[2, 2], duration_seconds=5,
                                  prefetch={"budget_ratio": 1, "advice_seconds": 1,
                                            "max_inflight_per_forwarder": 2})
        for when in (0, 0.8, 0.9):
            harness.clock.advance_to(when)
            for domain in range(12):
                harness.query(0, domain)
        self.tick(harness, 1)
        budget = harness.layers["manager"].prefetch_budgets[0]
        self.assertEqual(budget.active[0], 2)
        self.tick(harness, 1.9, passes=10)
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 12)
        self.assertEqual(budget.reserved[True], 12)
        self.assertEqual(budget.active[0], 0)

    def test_duplicate_budget_epoch_is_rejected_without_adding_credit(self):
        harness = ProtocolHarness("ReDNS-prefetch", forwarder_cluster_size=2)
        harness.query(0, 7)
        self.tick(harness, 1)
        manager = harness.layers["manager"]
        with self.assertRaisesRegex(ValueError, "already processed"):
            manager.handle_message(("prefetch_candidates", 0, 0, 1, (0, 1), {}, None))
        self.assertEqual(manager.prefetch_budgets[0].demand[True], 1)


class BudgetRuntimeTests(unittest.TestCase):
    def test_real_shards_enforce_cluster_credit_bounds_and_zero_ratio(self):
        for ratio in (0, 0.15, 1):
            with self.subTest(ratio=ratio):
                cfg = config(num_clients=12, num_forwarders=4, num_domains=12,
                             client_cache_size=0, forwarder_cache_size=12, forwarder_cluster_size=2,
                             workers={"client": 2, "forwarder": 2, "manager": 2},
                             warm_up_seconds=1, duration_seconds=3, queries_per_second=200,
                             time_scale=0.25, ttl_range=[2, 2],
                             prefetch={"budget_ratio": ratio, "report_seconds": 0.25,
                                       "advice_seconds": 0.5})
                with redirect_stdout(io.StringIO()):
                    result = RealtimeSimulation(cfg, "ReDNS-prefetch").run()
                self.assertEqual(result["total_queries"], result["client"]["queries"])
                self.assertEqual(result["ipc_messages_sent"], result["ipc_messages_received"])
                for row in result["managers"]:
                    fraction = Fraction(str(ratio))
                    allowance = row["prefetch_budget_demand"] * fraction.numerator // fraction.denominator
                    self.assertEqual(row["prefetch_budget_allowance"], allowance)
                    self.assertLessEqual(row["prefetch_budget_reserved"] - row["prefetch_budget_refunded"], allowance)
                if ratio == 0:
                    self.assertEqual(result["prefetch_authoritative_queries"], 0)
                else:
                    self.assertGreater(result["prefetch_authoritative_queries"], 0)


if __name__ == "__main__":
    unittest.main()
