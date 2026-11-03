from contextlib import redirect_stdout
import io
import unittest

from lib.config import validate_config
from lib.realtime import RealtimeSimulation
from src.cluster import ForwarderClusters
from src.records import Record
from tests.helpers import ProtocolHarness, config


class ClusterMembershipTests(unittest.TestCase):
    def test_consecutive_disjoint_groups_and_partial_final_group(self):
        clusters = ForwarderClusters(8, 3)
        self.assertEqual(list(clusters.members(0)), [0, 1, 2])
        self.assertEqual(list(clusters.members(5)), [3, 4, 5])
        self.assertEqual(list(clusters.members(7)), [6, 7])
        self.assertEqual(clusters.neighbors(2), (0, 1))
        self.assertFalse(clusters.can_query(2, 3))
        self.assertFalse(clusters.can_query(0, 0))
        self.assertTrue(clusters.can_query(0, 2))

    def test_singleton_and_oversized_groups(self):
        self.assertEqual(ForwarderClusters(3, 1).neighbors(1), ())
        self.assertEqual(ForwarderClusters(3, 10).neighbors(1), (0, 2))

    def test_invalid_counts_sizes_and_node_ids(self):
        for count, size in ((0, 3), (3, 0), (True, 3), (3, 1.5)):
            with self.assertRaises(ValueError):
                ForwarderClusters(count, size)
        for node in (-1, 3, True, 1.5):
            with self.assertRaises(ValueError):
                ForwarderClusters(3, 2).neighbors(node)

    def test_cluster_configuration_validation(self):
        self.assertEqual(validate_config({})["forwarder_cluster_size"], 1)
        self.assertEqual(validate_config({"num_forwarders": 3, "forwarder_cluster_size": 10})[
            "forwarder_cluster_size"], 10)
        for value in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                validate_config({"forwarder_cluster_size": value})
        for value in (-1, True, float("nan"), float("inf"), "fast"):
            with self.assertRaises(ValueError):
                validate_config({"forwarder_forwarder_RTT": value})


class ClusterProtocolTests(unittest.TestCase):
    def harness(self, model="ReDNS", **overrides):
        return ProtocolHarness(model, **{
            "num_clients": 12, "num_forwarders": 6, "forwarder_cluster_size": 3,
            "client_cache_size": 4, "forwarder_cache_size": 4,
            "forwarder_forwarder_RTT": 2, **overrides,
        })

    def store_forwarder(self, harness, node, domain=7, expiry=100):
        harness.layers["forwarder"]._select(node, True).insert_cache(Record(domain, expiry, 100))

    def store_client(self, harness, node, domain=7, expiry=100, measure=True):
        harness.layers["client"]._select(node, measure).insert_cache(Record(domain, expiry, 100))
        harness.drain()

    def test_neighbor_forwarder_hit_costs_one_forwarder_rtt(self):
        harness = self.harness()
        self.store_forwarder(harness, 1)
        self.store_forwarder(harness, 2)
        self.assertEqual(harness.query(0, 7), (100, 100, 4))
        nodes = harness.layers["forwarder"].nodes
        self.assertEqual(nodes[0].stats["cluster_forwarder_hits"], 1)
        self.assertEqual(nodes[0].stats["successful_reverse_queries"], 0)
        self.assertEqual(nodes[1].stats["cluster_cache_hits_served"], 1)
        self.assertEqual(nodes[1].stats["queries"], 0)
        self.assertEqual(nodes[2].stats["cluster_queries_received"], 0)
        self.assertEqual(harness.layers["authority"].authority.queries, 0)

    def test_neighbor_client_hit_refills_both_forwarders_and_preserves_ttl(self):
        harness = self.harness()
        self.store_client(harness, 1, expiry=50)
        self.store_forwarder(harness, 2)
        self.assertEqual(harness.query(0, 7), (50, 100, 5))
        nodes = harness.layers["forwarder"].nodes
        self.assertEqual(nodes[0].stats["cluster_client_hits"], 1)
        self.assertEqual(nodes[0].stats["successful_reverse_queries"], 1)
        self.assertEqual(nodes[1].stats["successful_reverse_queries"], 0)
        self.assertEqual(nodes[1].stats["cluster_child_hits"], 1)
        self.assertEqual(nodes[1].stats["peer_hits"], 0)
        self.assertEqual(nodes[2].stats["cluster_queries_received"], 0)
        self.assertEqual(nodes[0].cache[7].record.expires_at, 50)
        self.assertEqual(nodes[1].cache[7].record.expires_at, 50)
        self.assertEqual(harness.layers["client"].nodes[0].cache[7].record.expires_at, 50)
        self.assertEqual(harness.layers["authority"].authority.queries, 0)

    def test_own_forwarder_and_direct_clients_take_priority(self):
        harness = self.harness()
        self.store_client(harness, 6)
        self.store_forwarder(harness, 1)
        self.assertEqual(harness.query(0, 7)[2], 2)
        nodes = harness.layers["forwarder"].nodes
        self.assertEqual(nodes[0].stats["peer_hits"], 1)
        self.assertEqual(nodes[0].stats["cluster_queries"], 0)
        self.assertEqual(nodes[1].stats["cluster_queries_received"], 0)
        self.assertEqual(harness.query(6, 7)[2], 0)

    def test_stale_local_hint_tries_another_local_copy_before_neighbors(self):
        harness = self.harness()
        self.store_client(harness, 6)
        node = harness.layers["forwarder"].nodes[0]
        node.copy_directory.store(0, 7, 100)
        self.assertEqual(harness.query(0, 7)[2], 3)
        self.assertEqual(node.stats["peer_queries"], 2)
        self.assertEqual(node.stats["peer_misses"], 1)
        self.assertEqual(node.stats["cluster_queries"], 0)

    def test_failed_local_and_neighbor_client_probes_are_charged_before_next_hit(self):
        harness = self.harness()
        nodes = harness.layers["forwarder"].nodes
        nodes[0].copy_directory.store(6, 7, 100)
        nodes[1].copy_directory.store(7, 7, 100)
        harness.layers["forwarder"]._select(1, True)
        harness.layers["forwarder"]._advertise_region(1, 7)
        self.store_forwarder(harness, 2)
        self.assertEqual(harness.query(0, 7)[2], 8)
        self.assertEqual(nodes[0].stats["peer_misses"], 1)
        self.assertEqual(nodes[0].stats["cluster_queries"], 2)
        self.assertEqual(nodes[0].stats["cluster_misses"], 1)
        self.assertEqual(nodes[1].stats["cluster_child_misses"], 1)
        self.assertEqual(harness.layers["authority"].authority.queries, 0)

    def test_all_misses_fall_back_once_without_recursive_helper_queries(self):
        harness = self.harness()
        nodes = harness.layers["forwarder"].nodes
        for node_id in (0, 1, 2):
            nodes[node_id].copy_directory.store(node_id + 6, 7, 100)
            harness.layers["forwarder"]._select(node_id, True)
            harness.layers["forwarder"]._advertise_region(node_id, 7)
        self.assertEqual(harness.query(0, 7)[2], 119)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)
        self.assertEqual(nodes[0].stats["upstream_queries"], 1)
        for helper in nodes[1:3]:
            self.assertEqual(helper.stats["cluster_queries"], 0)
            self.assertEqual(helper.stats["upstream_queries"], 0)
            self.assertEqual(helper.stats["queries"], 0)

    def test_numerically_adjacent_forwarder_in_another_group_is_never_queried(self):
        harness = self.harness()
        self.store_forwarder(harness, 3)
        self.store_client(harness, 3)
        self.assertEqual(harness.query(2, 7)[2], 112)
        node = harness.layers["forwarder"].nodes[3]
        self.assertEqual(node.stats["cluster_queries_received"], 0)
        self.assertEqual(harness.layers["client"].nodes[3].stats["peer_queries_received"], 0)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)

    def test_cross_group_and_self_cluster_messages_are_rejected(self):
        for origin, helper in ((0, 3), (0, 0)):
            harness = self.harness()
            with self.assertRaises(ValueError):
                harness.layers["forwarder"]._message(("cluster_query", origin, 1, helper, 7, True))

    def test_expired_neighbor_cache_can_use_its_valid_client_copy(self):
        harness = self.harness()
        self.store_forwarder(harness, 1, expiry=5)
        self.store_client(harness, 1, expiry=50)
        harness.clock.advance_to(6)
        self.assertEqual(harness.query(0, 7)[2], 5)
        self.assertEqual(harness.layers["forwarder"].nodes[1].stats["expired_evictions"], 1)

    def test_warm_cluster_and_reverse_counts_stay_out_of_formal_statistics(self):
        harness = self.harness()
        self.store_client(harness, 1, measure=False)
        self.assertEqual(harness.query(0, 7, measure=False)[2], 5)
        layer = harness.layers["forwarder"]
        self.assertEqual(layer.warm_stats[0]["cluster_client_hits"], 1)
        self.assertEqual(layer.warm_stats[0]["successful_reverse_queries"], 1)
        self.assertEqual(layer.formal_stats[0]["cluster_hits"], 0)
        self.assertEqual(layer.formal_stats[0]["successful_reverse_queries"], 0)

    def test_baseline_ignores_cluster_setting(self):
        harness = self.harness("baseline")
        self.store_forwarder(harness, 1)
        self.assertEqual(harness.query(0, 7)[2], 111)
        self.assertEqual(harness.layers["forwarder"].nodes[0].stats["cluster_queries"], 0)

    def test_singleton_disables_neighbor_lookup(self):
        harness = self.harness(forwarder_cluster_size=1)
        self.store_forwarder(harness, 1)
        self.assertEqual(harness.query(0, 7)[2], 111)

    def test_final_partial_group_uses_its_actual_members_only(self):
        harness = self.harness(num_forwarders=5)
        self.store_forwarder(harness, 4)
        self.assertEqual(harness.query(3, 7)[2], 4)
        self.assertEqual(harness.layers["forwarder"].nodes[3].stats["cluster_queries"], 1)

    def test_background_transfer_remains_with_direct_clients(self):
        harness = self.harness(forwarder_cache_size=1)
        self.store_client(harness, 1)
        self.store_forwarder(harness, 0)
        self.store_forwarder(harness, 0, domain=8)
        harness.drain()
        clients = harness.layers["client"].nodes
        received = [i for i, client in enumerate(clients) if client.stats["records_received"]]
        self.assertEqual(len(received), 1)
        self.assertIn(received[0], (0, 6))


class ClusterRuntimeTests(unittest.TestCase):
    def test_real_queues_drain_with_neighbor_and_neighbor_client_hits(self):
        for policy in ("lru", "random"):
            cfg = config(
                num_clients=30, num_forwarders=6, forwarder_cluster_size=3,
                num_domains=30, client_cache_size=2, forwarder_cache_size=2,
                query_distribution="uniform", warm_up_seconds=1, duration_seconds=3,
                queries_per_second=400, time_scale=0.1, replacement_policy=policy,
            )
            with self.subTest(policy=policy), redirect_stdout(io.StringIO()):
                simulation = RealtimeSimulation(cfg, "ReDNS")
                result = simulation.run()
            self.assertEqual(result["forwarder_clusters"], [[0, 1, 2], [3, 4, 5]])
            self.assertGreater(result["cluster_forwarder_hits"], 0)
            self.assertGreater(result["cluster_client_hits"], 0)
            self.assertLessEqual(result["cluster_queries"], 2 * result["forwarder"]["misses"])
            self.assertGreaterEqual(result["successful_reverse_queries"], result["cluster_client_hits"])
            self.assertTrue(all(not process.is_alive() for process in simulation.processes.values()))
