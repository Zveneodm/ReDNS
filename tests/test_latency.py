import unittest

from sim.experiment import validate_config
from sim.latency import LatencyTracker
from sim.run_baseline import DeterministicSimulation as BaselineSimulation
from sim.run_ReDNS import DeterministicSimulation as ReDNSSimulation


class LatencyTests(unittest.TestCase):
    def simulation(self, model, **overrides):
        config = {
            "duration_seconds": 1, "queries_per_second": 2,
            "num_clients": 1, "num_forwarders": 1, "num_domains": 2,
            "client_cache_size": 1, "forwarder_cache_size": 1, "pub_cache_size": 1,
            "ttl_range": [100, 100], "min_transfer_ttl": 10,
            "client_forwarder_RTT": 1, "forwarder_pub_RTT": 10, "pub_auth_RTT": 50,
            **overrides,
        }
        simulation = model(config)
        simulation._choose_domain = lambda: simulation.domains[0]
        return simulation

    def test_each_cache_hit_level(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            for sizes, second_latency in (((1, 1, 1), 0), ((0, 1, 1), 1),
                                          ((0, 0, 1), 11), ((0, 0, 0), 61)):
                with self.subTest(model=model.experiment_name, sizes=sizes):
                    result = self.simulation(
                        model, client_cache_size=sizes[0],
                        forwarder_cache_size=sizes[1], pub_cache_size=sizes[2],
                    ).run()
                    self.assertEqual(result["request_latencies_ms"], [61, second_latency])
                    self.assertEqual(result["total_latency_ms"], 61 + second_latency)
                    self.assertEqual(result["average_latency_ms"], (61 + second_latency) / 2)
                    self.assertEqual(len(result["request_latencies_ms"]), result["total_queries"])

    def test_background_transfers_and_refresh_do_not_add_request_latency(self):
        for ttl in (100, 2):
            with self.subTest(ttl=ttl):
                simulation = self.simulation(ReDNSSimulation, ttl_range=[ttl, ttl])
                domains = iter(simulation.domains)
                simulation._choose_domain = lambda: next(domains)
                result = simulation.run()
                self.assertEqual(result["request_latencies_ms"], [61, 61])
                self.assertEqual(result["average_latency_ms"], 61)
                self.assertEqual(result["public_dns"]["records_transferred"], 1)
                self.assertEqual(result["forwarder"]["records_transferred"], 1)
                self.assertEqual(result["public_dns"]["refresh_queries"], int(ttl < 10))

    def test_background_refresh_preserves_existing_foreground_latency(self):
        simulation = self.simulation(ReDNSSimulation)
        public = simulation.public_server
        forwarder = simulation.forwarders[0]
        public.insert_cache("domain-00000.example", 5, 100)
        forwarder.insert_cache("domain-00000.example", 2, 100)
        simulation.latency_tracker.reset()
        simulation.latency_tracker.add_rtt(7)
        forwarder.insert_cache("domain-00001.example", 100, 100)
        self.assertEqual(simulation.latency_tracker.latency_ms, 7)
        self.assertEqual(public.stats["hits"], 1)
        self.assertEqual(simulation.authority.queries, 0)

    def test_background_refresh_miss_excludes_all_upstream_links(self):
        simulation = self.simulation(ReDNSSimulation, pub_cache_size=0, client_cache_size=2)
        first, second = simulation.domains
        simulation.forwarders[0].insert_cache(first, 2, 100)
        domains = iter((second, first))
        simulation._choose_domain = lambda: next(domains)
        result = simulation.run()
        # The first request performs one full foreground lookup and a separate
        # refresh of the evicted record. The second uses that pre-stored record.
        self.assertEqual(result["request_latencies_ms"], [61, 0])
        self.assertEqual(result["average_latency_ms"], 30.5)
        self.assertEqual(result["forwarder"]["refresh_queries"], 1)
        self.assertEqual(result["client"]["records_received"], 1)
        self.assertEqual(result["authoritative_queries"], 2)

    def test_request_uses_record_previously_transferred_to_forwarder(self):
        simulation = self.simulation(ReDNSSimulation, forwarder_cache_size=2)
        first, second = simulation.domains
        simulation.public_server.insert_cache(first, 100, 100)
        simulation.public_server.insert_cache(second, 100, 100)
        self.assertEqual(simulation.latency_tracker.latency_ms, 0)
        result = simulation.run()
        self.assertEqual(result["request_latencies_ms"], [1, 0])
        self.assertEqual(result["authoritative_queries"], 0)

    def test_nested_background_scopes_restore_latency_accounting_after_error(self):
        tracker = LatencyTracker()
        tracker.add_rtt(1)
        with self.assertRaises(ValueError):
            with tracker.background():
                tracker.add_rtt(50)
                with tracker.background():
                    tracker.add_rtt(10)
                tracker.add_rtt(1)
                raise ValueError("background refresh failed")
        self.assertEqual(tracker.latency_ms, 1)
        tracker.add_rtt(10)
        self.assertEqual(tracker.latency_ms, 11)
        tracker.reset()
        tracker.add_rtt(1)
        self.assertEqual(tracker.latency_ms, 1)

    def test_expired_records_require_full_lookup_again(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            with self.subTest(model=model.experiment_name):
                result = self.simulation(
                    model, duration_seconds=2, queries_per_second=1, ttl_range=[1, 1],
                ).run()
                self.assertEqual(result["request_latencies_ms"], [61, 61])

    def test_fractional_and_zero_rtts(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            for rtts, expected in (((0.1, 0.2, 0.3), 0.6), ((0, 0, 0), 0)):
                with self.subTest(model=model.experiment_name, rtts=rtts):
                    result = self.simulation(
                        model, client_cache_size=0, forwarder_cache_size=0, pub_cache_size=0,
                        client_forwarder_RTT=rtts[0], forwarder_pub_RTT=rtts[1], pub_auth_RTT=rtts[2],
                    ).run()
                    for latency in result["request_latencies_ms"]:
                        self.assertAlmostEqual(latency, expected)
                    self.assertAlmostEqual(result["average_latency_ms"], expected)

    def test_empty_workload(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            with self.subTest(model=model.experiment_name):
                result = self.simulation(model, queries_per_second=0).run()
                self.assertEqual(result["request_latencies_ms"], [])
                self.assertEqual(result["total_latency_ms"], 0)
                self.assertEqual(result["average_latency_ms"], 0)

    def test_invalid_rtts(self):
        for field in ("client_forwarder_RTT", "forwarder_pub_RTT", "pub_auth_RTT"):
            for value in (-1, True, "1", float("nan"), float("inf")):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    validate_config({field: value})


if __name__ == "__main__":
    unittest.main()
