from contextlib import redirect_stdout
import io
import unittest

from sim.clock import ManualClock
from sim.config import validate_config
from sim.realtime import RealtimeSimulation, simulation_roles
from src.manager import ClusterManager
from src.records import Record
from tests.helpers import ProtocolHarness, config


class ManagerDirectoryTests(unittest.TestCase):
    def setUp(self):
        self.clock = ManualClock()
        self.manager = ClusterManager(0, (0, 1, 2), self.clock)

    def test_one_merged_region_entry_per_domain(self):
        self.manager.update(1, 7, 50, 1)
        self.manager.update(1, 7, 100, 2)
        self.assertEqual(self.manager.entries, 1)
        self.assertEqual(self.manager.lookup(0, 7), [(1, 100, 2)])

    def test_earlier_deadline_update_is_accepted_when_its_version_is_newer(self):
        self.manager.update(1, 7, 100, 1)
        self.manager.update(1, 7, 20, 2)
        self.assertEqual(self.manager.lookup(0, 7), [(1, 20, 2)])

    def test_old_updates_and_delayed_feedback_cannot_remove_fresh_region(self):
        self.manager.update(1, 7, 10, 1)
        self.manager.update(1, 7, 100, 2)
        self.manager.invalidate(0, 1, 7, 1)
        self.manager.update(1, 7, 0, 1)
        self.assertEqual(self.manager.lookup(0, 7), [(1, 100, 2)])
        self.assertEqual(self.manager.stats["feedback_ignored"], 1)
        self.assertEqual(self.manager.stats["updates_ignored"], 1)

    def test_withdrawal_tombstone_blocks_old_advertisement(self):
        self.manager.update(1, 7, 100, 1)
        self.manager.update(1, 7, 0, 3)
        self.manager.update(1, 7, 100, 2)
        self.assertEqual(self.manager.lookup(0, 7), [])
        self.manager.update(1, 7, 100, 4)
        self.assertEqual(self.manager.lookup(0, 7), [(1, 100, 4)])

    def test_exact_feedback_invalidates_only_the_named_region(self):
        self.manager.update(1, 7, 100, 1)
        self.manager.update(2, 7, 100, 2)
        self.manager.invalidate(0, 1, 7, 1)
        self.assertEqual(self.manager.lookup(0, 7), [(2, 100, 2)])

    def test_lookup_excludes_requester_and_expired_hints(self):
        self.manager.update(0, 7, 100, 1)
        self.manager.update(1, 7, 5, 2)
        self.clock.advance_to(5)
        self.assertEqual(self.manager.lookup(0, 7), [])
        self.assertEqual(self.manager.entries, 1)

    def test_out_of_cluster_updates_queries_and_feedback_are_rejected(self):
        for call in (
            lambda: self.manager.update(3, 7, 100, 1),
            lambda: self.manager.lookup(3, 7),
            lambda: self.manager.invalidate(0, 3, 7, 1),
        ):
            with self.assertRaises(ValueError):
                call()


class ManagerProtocolTests(unittest.TestCase):
    def harness(self, **overrides):
        return ProtocolHarness(**{
            "num_clients": 12, "num_forwarders": 6, "forwarder_cluster_size": 3,
            "client_cache_size": 1, "forwarder_cache_size": 1,
            "forwarder_manager_RTT": 1, **overrides,
        })

    def cache(self, harness, role, node_id, domain=7, deadline=100, measure=True):
        harness.layers[role]._select(node_id, measure).insert_cache(Record(domain, deadline, 100))
        harness.drain()

    def test_own_and_client_copies_merge_and_survive_individual_removals(self):
        harness = self.harness()
        self.cache(harness, "forwarder", 1, deadline=100)
        self.cache(harness, "client", 1, deadline=50)
        self.cache(harness, "client", 7, deadline=150)
        manager = harness.layers["manager"].managers[0]
        self.assertEqual(len(manager.by_domain[7]), 1)
        self.assertEqual(manager.by_domain[7][1][0], 150)
        self.cache(harness, "forwarder", 1, domain=8)
        self.assertEqual(manager.by_domain[7][1][0], 150)
        self.cache(harness, "client", 7, domain=9)
        self.assertEqual(manager.by_domain[7][1][0], 50)

    def test_same_aggregate_deadline_does_not_send_duplicate_updates(self):
        harness = self.harness()
        self.cache(harness, "forwarder", 1, deadline=100)
        node = harness.layers["forwarder"].nodes[1]
        before = node.stats["manager_updates_sent"]
        self.cache(harness, "client", 1, deadline=50)
        self.assertEqual(node.stats["manager_updates_sent"], before)

    def test_manager_miss_skips_all_neighbor_probes_but_charges_lookup_rtt(self):
        harness = self.harness()
        self.assertEqual(harness.query(0, 7)[2], 112)
        node = harness.layers["forwarder"].nodes[0]
        self.assertEqual(node.stats["manager_queries"], 1)
        self.assertEqual(node.stats["manager_directory_misses"], 1)
        self.assertEqual(node.stats["cluster_queries"], 0)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)

    def test_only_advertised_forwarder_is_queried(self):
        harness = self.harness()
        self.cache(harness, "client", 2, deadline=80)
        self.assertEqual(harness.query(0, 7), (80, 100, 5))
        nodes = harness.layers["forwarder"].nodes
        self.assertEqual(nodes[0].stats["manager_directory_hits"], 1)
        self.assertEqual(nodes[0].stats["cluster_queries"], 1)
        self.assertEqual(nodes[1].stats["cluster_queries_received"], 0)
        self.assertEqual(nodes[2].stats["cluster_queries_received"], 1)
        self.assertEqual(nodes[0].stats["successful_reverse_queries"], 1)

    def test_stale_region_is_probed_once_then_repaired_and_next_hint_used(self):
        harness = self.harness()
        self.cache(harness, "client", 1)
        self.cache(harness, "forwarder", 2)
        # Simulate a lost removal notification, without changing directory data.
        del harness.layers["client"].nodes[1].cache[7]
        self.assertEqual(harness.query(0, 7)[2], 7)
        origin = harness.layers["forwarder"].nodes[0]
        self.assertEqual(origin.stats["cluster_queries"], 2)
        self.assertEqual(origin.stats["cluster_misses"], 1)
        self.assertEqual(origin.stats["manager_feedback_sent"], 1)
        manager = harness.layers["manager"].managers[0]
        self.assertNotIn(1, manager.by_domain.get(7, {}))
        self.assertIn(2, manager.by_domain[7])

    def test_expired_regional_summary_is_not_probed(self):
        harness = self.harness()
        self.cache(harness, "client", 1, deadline=5)
        harness.clock.advance_to(6)
        self.assertEqual(harness.query(0, 7)[2], 112)
        self.assertEqual(harness.layers["forwarder"].nodes[0].stats["cluster_queries"], 0)

    def test_region_update_is_applied_only_after_its_message_is_received(self):
        harness = self.harness()
        layer = harness.layers["forwarder"]
        layer._select(1, True).insert_cache(Record(7, 100, 100))
        manager = harness.layers["manager"].managers[0]
        self.assertNotIn(7, manager.by_domain)
        harness.drain()
        self.assertIn(1, manager.by_domain[7])

    def test_manager_negative_result_is_not_replaced_by_blind_scanning(self):
        harness = self.harness()
        layer = harness.layers["forwarder"]
        layer._select(1, True).insert_cache(Record(7, 100, 100))
        # Simulate an advertisement that has not reached the manager.
        layer.outbox.clear()
        self.assertEqual(harness.query(0, 7)[2], 112)
        self.assertEqual(layer.nodes[0].stats["cluster_queries"], 0)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)

    def test_zero_capacity_and_singleton_groups_do_not_advertise_or_query_manager(self):
        harness = self.harness(num_forwarders=4, num_clients=4)
        self.cache(harness, "client", 3)
        self.assertEqual(harness.query(3, 8)[2], 111)
        node = harness.layers["forwarder"].nodes[3]
        self.assertEqual(node.stats["manager_updates_sent"], 0)
        self.assertEqual(node.stats["manager_queries"], 0)

    def test_manager_lookup_rtt_is_configurable_and_can_be_zero(self):
        harness = self.harness(forwarder_manager_RTT=7)
        self.assertEqual(harness.query(0, 7)[2], 118)
        harness = self.harness(forwarder_manager_RTT=0)
        self.assertEqual(harness.query(0, 7)[2], 111)
        for value in (-1, True, float("inf")):
            with self.assertRaises(ValueError):
                validate_config({"forwarder_manager_RTT": value})


class ManagerRuntimeTests(unittest.TestCase):
    def test_manager_is_a_separate_owner_and_stats_survive_warmup_and_backpressure(self):
        cfg = config(
            num_clients=24, num_forwarders=6, forwarder_cluster_size=3,
            num_domains=30, client_cache_size=2, forwarder_cache_size=2,
            warm_up_seconds=1, duration_seconds=2, queries_per_second=400,
            time_scale=0.1, max_in_flight=8, query_distribution="uniform",
        )
        with redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(cfg, "ReDNS").run()
        self.assertTrue(result["manager_enabled"])
        self.assertEqual(set(result["workers"]), {"client", "forwarder", "authority", "manager"})
        self.assertEqual(len({row["pid"] for row in result["workers"].values()}), 4)
        self.assertEqual(len(result["managers"]), 2)
        self.assertGreater(result["manager_queries"], 0)
        self.assertGreater(result["manager_directory_hits"], 0)
        self.assertGreater(result["manager"]["updates_received"], 0)
        self.assertEqual(result["manager_queries"], result["manager"]["queries"])

    def test_baseline_and_size_one_do_not_start_manager_process(self):
        cfg = config(forwarder_cluster_size=3)
        self.assertEqual(simulation_roles(cfg, "baseline"), ("client", "forwarder", "authority"))
        cfg = config(forwarder_cluster_size=1)
        self.assertEqual(simulation_roles(cfg, "ReDNS"), ("client", "forwarder", "authority"))
