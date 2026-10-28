from contextlib import redirect_stdout
import io
import unittest
from unittest.mock import patch

from sim.clock import SimulationClock
from sim.run_baseline import DeterministicSimulation as BaselineSimulation
from sim.run_ReDNS import DeterministicSimulation as ReDNSSimulation
from src.baseline.Model import Resolver
from src.baseline.Record import Record as BaselineRecord
from src.Re_DNS.Model import ReDNS_Resolver
from src.Re_DNS.Record import Record as ReDNSRecord


class CountdownClock:
    """Reference clock that applies the original per-cache countdown loop."""

    def __init__(self, resolvers):
        self.resolvers = resolvers
        for resolver in resolvers:
            resolver.clock = None

    def advance(self):
        for resolver in self.resolvers:
            resolver.countdown()


class TTLDeadlineTests(unittest.TestCase):
    def config(self, **overrides):
        return {
            "warm_up_seconds": 3, "duration_seconds": 7, "queries_per_second": 17,
            "num_clients": 5, "num_forwarders": 2, "num_domains": 19,
            "client_cache_size": 1, "forwarder_cache_size": 2, "pub_cache_size": 3,
            "min_transfer_ttl": 3,
            "ttl_distribution": {"values": [0, 1, 3, 30], "probabilities": [0.1, 0.3, 0.3, 0.3]},
            **overrides,
        }

    @staticmethod
    def capture_seconds(simulation):
        snapshots = []
        run_second = simulation._run_second

        def capture(*, measure):
            run_second(measure=measure)
            snapshots.append([
                (
                    [(name, record.ttl, record.auth_ttl) for name, record in resolver.cache.items()],
                    dict(resolver.stats),
                )
                for resolver in simulation.resolvers
            ])

        simulation._run_second = capture
        return snapshots

    def test_matches_countdown_at_every_tick(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            for distribution in ("zipf", "uniform"):
                for sizes in ((1, 2, 3), (0, 0, 0), (2, 0, 3)):
                    with self.subTest(model=model.experiment_name, distribution=distribution, sizes=sizes):
                        config = self.config(
                            query_distribution=distribution, client_cache_size=sizes[0],
                            forwarder_cache_size=sizes[1], pub_cache_size=sizes[2],
                        )
                        deadline = model(config)
                        countdown = model(config)
                        countdown.clock = CountdownClock(countdown.resolvers)
                        actual_ticks = self.capture_seconds(deadline)
                        expected_ticks = self.capture_seconds(countdown)
                        with redirect_stdout(io.StringIO()):
                            actual = deadline.run()
                            expected = countdown.run()
                        self.assertEqual(actual, expected)
                        self.assertEqual(actual_ticks, expected_ticks)
                        self.assertEqual(deadline.traffic_rng.getstate(), countdown.traffic_rng.getstate())
                        self.assertEqual(deadline.clock.now, 10)

    def test_expiry_boundary_and_detached_copies(self):
        for record_class in (BaselineRecord, ReDNSRecord):
            with self.subTest(record=record_class.__module__):
                clock = SimulationClock()
                record = record_class("a", 2, clock=clock)
                clock.advance()
                answer = record.copy()
                self.assertEqual((record.ttl, record.TTL, answer.ttl), (1, 1, 1))
                clock.advance()
                self.assertTrue(record.is_expired())
                clock.advance(100)
                self.assertEqual(record.ttl, 0)
                self.assertEqual(answer.ttl, 1)
                record.TTL = 7
                clock.advance(2)
                self.assertEqual(record.ttl, 5)
                record.ttl = 4
                record.countdown()
                self.assertEqual(record.TTL, 3)
                self.assertEqual(record.auth_ttl, 2)

    def test_standalone_countdown(self):
        for record_class in (BaselineRecord, ReDNSRecord):
            with self.subTest(record=record_class.__module__):
                record = record_class("a", 5)
                record.countdown(3)
                self.assertEqual((record.TTL, record.ttl, record.auth_ttl), (2, 2, 5))
                record.countdown(10)
                self.assertEqual(record.ttl, 0)

    def test_expired_entries_are_still_removed_lazily(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            with self.subTest(model=model.experiment_name):
                simulation = model(self.config(ttl_distribution=None, ttl_range=[30, 30]))
                public = simulation.public_server
                name = simulation.domains[0]
                public.insert_cache(name, 1, 30)
                simulation.clock.advance()
                self.assertEqual(public.metrics()["cache_entries"], 1)
                self.assertEqual(public.metrics()["live_cache_entries"], 0)
                self.assertEqual(public.stats["expired_evictions"], 0)
                self.assertEqual(public.handle_query(name).ttl, 30)
                self.assertEqual(public.stats["expired_evictions"], 1)

    def test_transfer_uses_remaining_ttl_and_refresh_threshold(self):
        for ttl, elapsed, auth_ttl, expected, queries in (
            (100, 7, 100, 93, 0), (12, 3, 100, 100, 1), (9, 1, 5, 10, 1),
        ):
            with self.subTest(ttl=ttl, auth_ttl=auth_ttl):
                simulation = ReDNSSimulation(self.config(
                    num_clients=1, num_forwarders=1, num_domains=2,
                    pub_cache_size=1, forwarder_cache_size=3, min_transfer_ttl=10,
                    ttl_distribution={"values": [auth_ttl], "probabilities": [1]},
                ))
                public = simulation.public_server
                first, second = simulation.domains
                public.insert_cache(first, ttl, auth_ttl)
                simulation.clock.advance(elapsed)
                public.insert_cache(second, 100, 100)
                received = simulation.forwarders[0].cache[first]
                self.assertEqual((received.ttl, received.auth_ttl), (expected, auth_ttl))
                self.assertEqual(simulation.authority.queries, queries)
                simulation.clock.advance()
                self.assertEqual(received.ttl, expected - 1)

    def test_simulation_never_calls_per_cache_countdown(self):
        with patch.object(Resolver, "countdown", side_effect=AssertionError("cache scan")), \
                patch.object(ReDNS_Resolver, "countdown", side_effect=AssertionError("cache scan")):
            for model in (BaselineSimulation, ReDNSSimulation):
                with self.subTest(model=model.experiment_name), redirect_stdout(io.StringIO()):
                    model(self.config()).run()


if __name__ == "__main__":
    unittest.main()
