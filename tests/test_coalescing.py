"""Concurrent foreground misses share one resolution per forwarder/cache key."""

import unittest
from unittest.mock import patch

from src.records import Record
from tests.helpers import ProtocolHarness


MODELS = ("baseline", "baseline-prefetch", "ReDNS", "ReDNS-prefetch")


class CoalescingTests(unittest.TestCase):
    def harness(self, model="baseline", **overrides):
        return ProtocolHarness(model, **{
            "num_clients": 6, "num_forwarders": 2, "client_cache_size": 2,
            "forwarder_cache_size": 2, "ttl_range": [100, 100], **overrides})

    def issue(self, harness, client, domain, *, measure=True, trace=None):
        replies = []
        harness.layers["client"]._query(client, domain, True, measure, replies.append, trace=trace)
        return replies

    def test_two_clients_same_forwarder_share_one_authority_query(self):
        for model in MODELS:
            with self.subTest(model=model):
                harness = self.harness(model)
                first = self.issue(harness, 0, 7)
                second = self.issue(harness, 2, 7)
                harness.drain()
                self.assertEqual(harness.layers["authority"].authority.queries, 1)
                self.assertEqual(first, second)
                self.assertEqual(first[0][2], 111)
                node = harness.layers["forwarder"]._select(0, True)
                self.assertEqual((node.stats["queries"], node.stats["misses"], node.stats["hits"]), (2, 2, 0))
                self.assertEqual(node.stats["upstream_queries"], 1)
                self.assertEqual(node.stats["coalesced_queries"], 1)
                self.assertEqual(node.stats["coalesced_authority_waits"], 1)
                self.assertEqual(node.stats["coalesced_wait_latency_ms"], 110)
                for client in (0, 2):
                    self.assertEqual(harness.layers["client"].nodes[client].cache[7].record.expires_at,
                                     first[0][0])
                self.assertFalse(harness.layers["forwarder"].inflight_queries)

    def test_zero_capacity_or_zero_ttl_still_coalesces_only_while_pending(self):
        for model in MODELS:
            for options in ({"forwarder_cache_size": 0}, {"ttl_range": [0, 0]}):
                with self.subTest(model=model, options=options):
                    harness = self.harness(model, client_cache_size=0, **options)
                    self.issue(harness, 0, 7)
                    self.issue(harness, 2, 7)
                    harness.drain()
                    self.assertEqual(harness.layers["authority"].authority.queries, 1)
                    self.assertFalse(harness.layers["forwarder"].inflight_queries)
                    self.issue(harness, 0, 7)
                    harness.drain()
                    self.assertEqual(harness.layers["authority"].authority.queries, 2)

    def test_different_forwarders_and_cache_keys_do_not_merge(self):
        for model in MODELS:
            with self.subTest(model=model):
                harness = self.harness(model)
                replies = [self.issue(harness, client, domain)
                           for client, domain in ((0, 7), (2, 7), (1, 7), (0, 8))]
                harness.drain()
                self.assertEqual(harness.layers["authority"].authority.queries, 3)
                self.assertTrue(all(len(reply) == 1 for reply in replies))

    def test_shared_resolution_preserves_each_request_authority_trace(self):
        for model in MODELS:
            with self.subTest(model=model):
                harness = self.harness(model)
                traces = [{}, {}]
                replies = [self.issue(harness, client, 7, trace=trace)
                           for client, trace in zip((0, 2), traces)]
                harness.drain()
                self.assertEqual(traces, [{"authority_query": True}, {"authority_query": False}])
                self.assertEqual(replies[0], replies[1])

    def test_peer_and_cluster_results_are_shared_and_fill_forwarder_once(self):
        for model in ("ReDNS", "ReDNS-prefetch"):
            for source in ("peer", "cluster_forwarder", "cluster_client"):
                with self.subTest(model=model, source=source):
                    harness = self.harness(model, forwarder_cluster_size=2)
                    role, node_id = {"peer": ("client", 4), "cluster_forwarder": ("forwarder", 1),
                                     "cluster_client": ("client", 3)}[source]
                    harness.layers[role]._select(node_id, True).insert_cache(Record(7, 100, 100))
                    harness.drain()
                    node = harness.layers["forwarder"].nodes[0]
                    with patch.object(node, "insert_cache", wraps=node.insert_cache) as store:
                        first, second = self.issue(harness, 0, 7), self.issue(harness, 2, 7)
                        harness.drain()
                        self.assertEqual(store.call_count, 1)
                    self.assertEqual(harness.layers["authority"].authority.queries, 0)
                    self.assertEqual(first, second)
                    self.assertEqual(first[0][2], {"peer": 2, "cluster_forwarder": 4,
                                                  "cluster_client": 5}[source])
                    self.assertEqual(node.stats[f"coalesced_{source}_waits"], 1)
                    self.assertEqual(node.stats["successful_reverse_queries"],
                                     0 if source == "cluster_forwarder" else 2)
                    self.assertEqual(node.stats["peer_queries" if source == "peer" else "cluster_queries"], 1)
                    self.assertFalse(harness.layers["forwarder"].inflight_queries)

    def test_reverse_query_identity_is_evaluated_for_each_waiter(self):
        harness = self.harness("ReDNS")
        harness.layers["client"]._select(4, True).insert_cache(Record(7, 100, 100))
        harness.drain()
        forwarder = harness.layers["forwarder"]
        replies = [[], [], []]
        for requester, answers in zip((4, 0, 4), replies):
            forwarder._query(0, 7, True, True, answers.append, requester=requester)
        harness.drain()
        self.assertTrue(all(len(answers) == 1 for answers in replies))
        self.assertEqual(forwarder.nodes[0].stats["peer_queries"], 1)
        self.assertEqual(forwarder.nodes[0].stats["successful_reverse_queries"], 1)

    def test_late_waiter_pays_only_links_uncompleted_at_join(self):
        harness = self.harness("ReDNS", forwarder_cluster_size=2)
        first = self.issue(harness, 0, 7)
        client, forwarder, manager = (harness.layers[role] for role in ("client", "forwarder", "manager"))
        role, query = client.outbox.popleft()
        self.assertEqual(role, "forwarder")
        forwarder._message(query)
        role, lookup = forwarder.outbox.popleft()
        self.assertEqual(role, "manager")
        manager._message(lookup)
        role, located = manager.outbox.popleft()
        forwarder._message(located)
        self.assertEqual(forwarder.inflight_queries[(0, 7)].completed_rtt, 1)
        second = self.issue(harness, 2, 7)
        _, query = client.outbox.popleft()
        forwarder._message(query)
        harness.drain()
        self.assertEqual(first[0][2], 112)
        self.assertEqual(second[0][2], 111)
        self.assertEqual(forwarder.nodes[0].stats["coalesced_wait_latency_ms"], 110)
        self.assertEqual(harness.layers["authority"].authority.queries, 1)

    def test_warmup_and_formal_waiters_keep_separate_accounting(self):
        for model in MODELS:
            for leader_formal in (False, True):
                with self.subTest(model=model, leader_formal=leader_formal):
                    harness = self.harness(model)
                    traces = [{}, {}]
                    self.issue(harness, 0, 7, measure=leader_formal, trace=traces[0])
                    self.issue(harness, 2, 7, measure=not leader_formal, trace=traces[1])
                    harness.drain()
                    stats = harness.layers["forwarder"]._select(0, True).stats
                    self.assertEqual(stats["queries"], 1)
                    self.assertEqual(stats["misses"], 1)
                    self.assertEqual(stats["upstream_queries"], int(leader_formal))
                    self.assertEqual(stats["coalesced_queries"], int(not leader_formal))
                    self.assertEqual(harness.layers["authority"].authority.queries, int(leader_formal))
                    self.assertEqual(traces, [{"authority_query": True}, {"authority_query": False}])

    def test_cluster_helper_does_not_wait_for_foreground_resolution(self):
        harness = self.harness("ReDNS", forwarder_cluster_size=2)
        first = self.issue(harness, 0, 7)
        forwarder, manager = harness.layers["forwarder"], harness.layers["manager"]
        _, query = harness.layers["client"].outbox.popleft()
        forwarder._message(query)
        _, lookup = forwarder.outbox.popleft()
        manager._message(lookup)
        _, located = manager.outbox.popleft()
        forwarder._message(located)
        authority_request = forwarder.outbox.popleft()
        replies = []
        token = forwarder._new_token()
        forwarder.pending[token] = lambda answer, cost: replies.append((answer, cost))
        forwarder._serve_cluster(1, token, 0, 7, True)
        harness.drain()
        self.assertEqual(replies, [(None, 0.0)])
        self.assertFalse(first)
        self.assertIn((0, 7), forwarder.inflight_queries)
        forwarder.outbox.append(authority_request)
        harness.drain()
        self.assertEqual(len(first), 1)

    def test_flight_is_released_before_reentrant_query_or_failed_send(self):
        harness = self.harness(client_cache_size=0, forwarder_cache_size=0)
        forwarder = harness.layers["forwarder"]
        replies = []

        def received(answer):
            replies.append(answer)
            forwarder._query(0, 7, True, True, replies.append)

        forwarder._query(0, 7, True, True, received)
        harness.drain()
        self.assertEqual(len(replies), 2)
        self.assertEqual(harness.layers["authority"].authority.queries, 2)
        self.assertFalse(forwarder.inflight_queries)
        with patch.object(forwarder, "_send", side_effect=RuntimeError("transport failed")):
            with self.assertRaisesRegex(RuntimeError, "transport failed"):
                forwarder._query(0, 8, True, True, replies.append)
        self.assertFalse(forwarder.inflight_queries)

    def test_different_client_workers_share_forwarder_owned_flight(self):
        from tests.test_parallel import ShardedHarness

        for model in MODELS:
            with self.subTest(model=model):
                harness = ShardedHarness(model, num_clients=9, num_forwarders=3,
                                         workers={"client": 2, "forwarder": 2, "manager": 2})
                replies, traces = [[], []], [{}, {}]
                self.assertIsNot(harness.owner("client", 0), harness.owner("client", 3))
                for client, answers, trace in zip((0, 3), replies, traces):
                    harness.owner("client", client)._query(client, 7, True, True, answers.append, trace=trace)
                harness.drain()
                self.assertEqual(harness.owner("authority", 0).authority.queries, 1)
                self.assertTrue(all(len(answers) == 1 for answers in replies))
                self.assertEqual(harness.owner("forwarder", 0).nodes[0].stats["coalesced_queries"], 1)
                self.assertEqual(traces, [{"authority_query": True}, {"authority_query": False}])

    def test_large_waiter_batch_receives_exactly_one_reply_each(self):
        for model in MODELS:
            with self.subTest(model=model):
                harness = self.harness(model)
                forwarder = harness.layers["forwarder"]
                replies = [[] for _ in range(100)]
                for answers in replies:
                    forwarder._query(0, 7, True, True, answers.append)
                harness.drain()
                self.assertTrue(all(answers == replies[0] for answers in replies))
                self.assertEqual(len(replies[0]), 1)
                self.assertEqual(harness.layers["authority"].authority.queries, 1)
                self.assertEqual(forwarder.nodes[0].stats["coalesced_queries"], 99)
                self.assertFalse(forwarder.inflight_queries)

    def test_real_sharded_runtime_coalesces_without_dropping_foreground_requests(self):
        from contextlib import redirect_stdout
        import io

        from lib.metrics import validate_results
        from lib.realtime import RealtimeSimulation
        from tests.helpers import config

        cfg = config(num_clients=12, num_forwarders=3, num_domains=1,
                     workers={"client": 2, "forwarder": 2, "manager": 2}, forwarder_cluster_size=3,
                     client_cache_size=0, forwarder_cache_size=0, warm_up_seconds=1,
                     duration_seconds=2, queries_per_second=800, max_in_flight=256, time_scale=0.02)
        for model in MODELS:
            with self.subTest(model=model), redirect_stdout(io.StringIO()):
                simulation = RealtimeSimulation(cfg, model)
                result = simulation.run()
                validate_results(result)
                self.assertGreater(result["forwarder"]["coalesced_queries"], 0)
                self.assertEqual(result["authoritative_queries"] +
                                 result["forwarder"]["coalesced_authority_waits"], result["total_queries"])
                self.assertEqual(sum(result["request_authority_queries"]), result["authoritative_queries"])
                self.assertEqual(len(result["request_latencies_ms"]), result["total_queries"])
                self.assertTrue(all(not process.is_alive() for process in simulation.processes.values()))


if __name__ == "__main__":
    unittest.main()
