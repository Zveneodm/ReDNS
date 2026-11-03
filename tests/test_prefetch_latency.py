"""Background prefetch must never be charged to foreground RTT samples."""

from time import perf_counter_ns
import unittest

from tests.helpers import ProtocolHarness


class PrefetchLatencyTests(unittest.TestCase):
    def harness(self, **overrides):
        harness = ProtocolHarness("ReDNS-prefetch", **{
            "num_clients": 1, "num_forwarders": 1, "forwarder_cluster_size": 1,
            "client_cache_size": 0, "forwarder_cache_size": 8,
            "duration_seconds": 20, "ttl_range": [100, 100], **overrides})
        # Exercise the actual arrival driver's sample recording with a manual
        # protocol clock. Its wall-time diagnostics need an epoch and scale.
        harness.clock.start_ns = perf_counter_ns()
        harness.clock.scale = 1
        return harness

    def test_background_batch_has_no_foreground_samples_or_latency_charge(self):
        harness = self.harness(forwarder_authoritative_RTT=100000)
        forwarder = harness.layers["forwarder"]
        driver = harness.layers["client"]
        for domain in (1, 2, 3, 4):
            forwarder.handle_message(("prefetch_task", 0, domain, 0, True))
        forwarder.prefetch_tick()
        harness.drain()
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 4)
        self.assertEqual(harness.layers["authority"].authority.queries, 0)
        self.assertEqual(forwarder.nodes[0].stats["queries"], 0)
        self.assertEqual(len(driver.latencies), 0)
        driver._issue(0, 1, 0)
        harness.drain()
        snapshot = driver._arrival_snapshot()
        self.assertEqual(snapshot["total_queries"], 1)
        self.assertEqual(list(snapshot["request_latencies_ms"]), [1])
        self.assertEqual(snapshot["total_latency_ms"], 1)
        self.assertEqual(snapshot["average_latency_ms"], 1)

    def test_foreground_miss_does_not_wait_for_or_add_outstanding_prefetch(self):
        harness = self.harness()
        forwarder = harness.layers["forwarder"]
        driver = harness.layers["client"]
        forwarder.handle_message(("prefetch_task", 0, 7, 0, True))
        forwarder.prefetch_tick()
        role, background_request = forwarder.outbox.popleft()
        self.assertEqual(role, "authority")
        self.assertEqual(background_request[0], "prefetch_query")
        # Hold the prefetch request in transit while a user asks for that name.
        driver._issue(0, 7, 0)
        harness.drain()
        self.assertEqual(list(driver.latencies), [111])
        self.assertEqual(harness.layers["authority"].authority.queries, 1)
        self.assertIn((0, 7), forwarder.prefetch_fetching)
        # A late background completion must not add a sample or alter the one
        # already completed foreground request, even after seven simulated seconds.
        harness.clock.advance_to(7)
        harness.layers[role]._message(background_request)
        harness.drain()
        self.assertEqual(harness.layers["authority"].authority.prefetch_queries, 1)
        self.assertEqual(list(driver.latencies), [111])
        snapshot = driver._arrival_snapshot()
        self.assertEqual(snapshot["total_queries"], 1)
        self.assertEqual(snapshot["total_latency_ms"], 111)
        self.assertEqual(snapshot["average_latency_ms"], 111)


if __name__ == "__main__":
    unittest.main()
