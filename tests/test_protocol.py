from collections import deque
from queue import Full
import unittest

from src.models import BaselineForwarder, ReDNSForwarder
from src.records import Record
from tests.helpers import ProtocolHarness


class ProtocolTests(unittest.TestCase):
    def test_cold_authority_forwarder_and_client_hit_latencies(self):
        for model in ("baseline", "ReDNS"):
            harness = ProtocolHarness(model)
            self.assertEqual(harness.query(0, 7)[2], 111)
            self.assertEqual(harness.query(2, 7)[2], 1)
            self.assertEqual(harness.query(0, 7)[2], 0)
            self.assertEqual(harness.layers["authority"].authority.queries, 1)
            self.assertTrue(all(node.stats["successful_reverse_queries"] == 0
                                for node in harness.layers["forwarder"].nodes))

    def test_forwarder_miss_reads_actual_client_copy(self):
        harness = ProtocolHarness()
        harness.layers["client"]._select(0, True).insert_cache(Record(7, 100, 100))
        harness.drain()
        expiry, ttl, latency = harness.query(2, 7)
        self.assertEqual((expiry, ttl, latency), (100, 100, 2))
        self.assertEqual(harness.layers["authority"].authority.queries, 0)
        forwarder = harness.layers["forwarder"].nodes[0]
        self.assertEqual(forwarder.stats["peer_hits"], 1)
        self.assertEqual(forwarder.stats["successful_reverse_queries"], 1)
        self.assertEqual(forwarder.stats["misses"], 1)
        self.assertEqual(forwarder.stats["hits"], 0)
        self.assertEqual(forwarder.check_cache(7).expires_at, 100)

    def test_reverse_success_counts_requests_instead_of_distinct_domains(self):
        harness = ProtocolHarness(client_cache_size=1, forwarder_cache_size=0)
        client_layer = harness.layers["client"]
        client_layer._select(0, True).insert_cache(Record(7, 100, 100))
        harness.drain()
        self.assertEqual(harness.query(2, 7)[2], 2)
        client_layer._select(2, True).insert_cache(Record(8, 100, 100))
        harness.drain()
        self.assertEqual(harness.query(2, 7)[2], 2)
        self.assertEqual(harness.layers["forwarder"].nodes[0].stats["successful_reverse_queries"], 2)

    def test_reverse_success_excludes_requester_as_cache_source(self):
        harness = ProtocolHarness()
        harness.layers["client"]._select(0, True).insert_cache(Record(7, 100, 100))
        harness.drain()
        replies = []
        # Explicitly exercise a directory hint pointing to the requester itself.
        layer = harness.layers["forwarder"]
        layer._query(0, 7, True, True, replies.append, requester=0)
        harness.drain()
        self.assertEqual(len(replies), 1)
        self.assertEqual(layer.nodes[0].stats["peer_hits"], 1)
        self.assertEqual(layer.nodes[0].stats["successful_reverse_queries"], 0)

    def test_warm_reverse_success_does_not_enter_formal_counter(self):
        harness = ProtocolHarness(forwarder_cache_size=0)
        harness.layers["client"]._select(0, False).insert_cache(Record(7, 100, 100))
        harness.drain()
        self.assertEqual(harness.query(2, 7, measure=False)[2], 2)
        layer = harness.layers["forwarder"]
        self.assertEqual(layer.warm_stats[0]["successful_reverse_queries"], 1)
        self.assertEqual(layer.formal_stats[0]["successful_reverse_queries"], 0)

    def test_baseline_does_not_advertise_or_query_client_copies(self):
        harness = ProtocolHarness("baseline")
        harness.layers["client"]._select(0, True).insert_cache(Record(7, 100, 100))
        self.assertFalse(harness.layers["client"].outbox)
        self.assertEqual(harness.query(2, 7)[2], 111)
        forwarder = harness.layers["forwarder"].nodes[0]
        self.assertIsInstance(forwarder, BaselineForwarder)
        self.assertFalse(hasattr(forwarder, "copy_directory"))
        self.assertIsNone(forwarder.on_evict)

    def test_copy_directory_is_scoped_to_a_clients_forwarder(self):
        harness = ProtocolHarness()
        harness.layers["client"]._select(0, True).insert_cache(Record(7, 100, 100))
        harness.drain()
        forwarders = harness.layers["forwarder"].nodes
        self.assertEqual(forwarders[0].peer_target(7), 0)
        self.assertIsNone(forwarders[1].peer_target(7))
        self.assertEqual(harness.query(1, 7)[2], 111)

    def test_stale_hint_costs_a_probe_before_authority_fallback(self):
        harness = ProtocolHarness()
        forwarder = harness.layers["forwarder"].nodes[0]
        forwarder.copy_directory.store(0, 7, 100)
        self.assertEqual(harness.query(2, 7)[2], 112)
        self.assertEqual(forwarder.stats["peer_misses"], 1)
        self.assertEqual(forwarder.stats["successful_reverse_queries"], 0)
        self.assertEqual(harness.layers["client"].nodes[0].stats["upstream_queries"], 0)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)

    def test_expired_directory_hint_is_not_probed(self):
        harness = ProtocolHarness()
        forwarder = harness.layers["forwarder"].nodes[0]
        forwarder.copy_directory.store(0, 7, 10)
        harness.clock.advance_to(10)
        self.assertEqual(harness.query(2, 7)[2], 111)
        self.assertEqual(forwarder.stats["peer_queries"], 0)

    def test_client_replacement_and_expiry_remove_actual_hints(self):
        harness = ProtocolHarness(client_cache_size=1)
        client = harness.layers["client"]._select(0, True)
        forwarder = harness.layers["forwarder"].nodes[0]
        client.insert_cache(Record(7, 10, 10))
        harness.drain()
        client.insert_cache(Record(8, 20, 20))
        harness.drain()
        self.assertIsNone(forwarder.peer_target(7))
        self.assertEqual(forwarder.peer_target(8), 0)
        harness.clock.advance_to(20)
        client.check_cache(8)
        harness.drain()
        self.assertFalse(forwarder.copy_directory.by_name)

    def test_transfer_reports_only_after_receiver_actually_stores(self):
        harness = ProtocolHarness(forwarder_cache_size=1)
        layer = harness.layers["forwarder"]
        node = layer._select(0, True)
        node.insert_cache(Record(7, 50, 100))
        node.insert_cache(Record(8, 100, 100))
        self.assertIsNone(node.peer_target(7))
        self.assertEqual(node.stats["records_transferred"], 1)
        harness.drain()
        target = node.peer_target(7)
        self.assertIn(target, (0, 2))
        self.assertEqual(harness.layers["client"].nodes[target].check_cache(7).expires_at, 50)
        self.assertEqual(node.copy_directory.deadline(target, 7), 50)

    def test_receiver_rechecks_capacity_when_occupancy_hint_is_stale(self):
        harness = ProtocolHarness(client_cache_size=1, forwarder_cache_size=1)
        for client_id in (0, 2):
            harness.layers["client"]._select(client_id, True).insert_cache(Record(1, 100, 100))
        # The parent has not received these store notifications yet.
        node = harness.layers["forwarder"]._select(0, True)
        node.insert_cache(Record(7, 100, 100))
        node.insert_cache(Record(8, 100, 100))
        harness.drain()
        clients = harness.layers["client"].nodes
        self.assertEqual(set(clients[0].cache), {1})
        self.assertEqual(set(clients[2].cache), {1})
        self.assertEqual(sum(c.stats["transfers_rejected_full"] for c in clients), 1)

    def test_delayed_peer_failure_preserves_newer_copy_notice(self):
        harness = ProtocolHarness()
        layer = harness.layers["forwarder"]
        node = layer._select(0, True)
        node.copy_directory.store(0, 7, 10)
        replies = []
        layer._query(0, 7, True, True, replies.append)
        node.copy_directory.store(0, 7, 100)
        # Deliver the already pending negative reply after the newer notice.
        layer.pending.pop(layer.token)(None)
        self.assertEqual(node.copy_directory.deadline(0, 7), 100)

    def test_warm_notifications_are_not_counted_as_formal_maintenance(self):
        harness = ProtocolHarness(forwarder_cache_size=0)
        harness.query(0, 7, measure=False)
        forwarder = harness.layers["forwarder"]
        self.assertEqual(forwarder.formal_stats[0]["directory_stores_received"], 0)
        self.assertGreater(forwarder.warm_stats[0]["directory_stores_received"], 0)
        self.assertEqual(harness.query(2, 7)[2], 2)
        self.assertEqual(forwarder.formal_stats[0]["peer_hits"], 1)
        self.assertEqual(forwarder.formal_stats[0]["successful_reverse_queries"], 1)

    def test_misrouted_cache_notification_is_rejected(self):
        harness = ProtocolHarness()
        with self.assertRaises(RuntimeError):
            harness.layers["forwarder"]._message(("cache_stored", 1, 0, 7, 100, True))

    def test_empty_forwarder_remains_valid_when_more_forwarders_than_clients(self):
        harness = ProtocolHarness(num_clients=1, num_forwarders=3, forwarder_cache_size=1)
        node = harness.layers["forwarder"]._select(2, True)
        self.assertIsInstance(node, ReDNSForwarder)
        node.insert_cache(Record(7, 100, 100))
        node.insert_cache(Record(8, 100, 100))
        self.assertEqual(node.stats["records_transferred"], 0)


class FakeQueue:
    def __init__(self, full_once=False):
        self.full_once = full_once
        self.messages = []

    def put_nowait(self, message):
        if self.full_once:
            self.full_once = False
            raise Full
        self.messages.append(message)


class QueueTests(unittest.TestCase):
    def test_backpressure_preserves_order_across_more_than_one_flush_batch(self):
        layer = ProtocolHarness().layers["forwarder"]
        blocked, other = FakeQueue(True), FakeQueue()
        layer.inboxes = {"client": blocked, "authority": other}
        layer.outbox = deque(("client", index) for index in range(300))
        layer.outbox.append(("authority", "independent"))
        layer._flush()
        self.assertEqual(blocked.messages, [])
        self.assertEqual(other.messages, ["independent"])
        while layer.outbox:
            layer._flush()
        self.assertEqual(blocked.messages, list(range(300)))
