from collections import Counter
import math
import random
import statistics
import unittest
from unittest.mock import patch

from sim.traffic.arrivals import Workload
from lib.clock import ManualClock, RealtimeClock
from sim.traffic.preferences import HierarchicalZipf
from src.models import AuthoritativeServer
from src.records import Record
from tests.helpers import ProtocolHarness, config


class WorkloadTests(unittest.TestCase):
    def test_same_seed_produces_identical_arrivals_clients_and_domains(self):
        for regional in (False, True):
            cfg = config(regional_preferences={"enabled": regional})
            first, second = Workload(cfg), Workload(cfg)
            self.assertEqual([first.pop() for _ in range(1000)], [second.pop() for _ in range(1000)])

    def test_replacement_policy_does_not_change_offered_workload(self):
        first = Workload(config(replacement_policy="lru"))
        second = Workload(config(replacement_policy="random"))
        self.assertEqual([first.pop() for _ in range(1000)], [second.pop() for _ in range(1000)])

    def test_zero_rate_has_no_arrivals(self):
        self.assertEqual(Workload(config(queries_per_second=0)).next_time, math.inf)

    def test_poisson_rate_variance_uniform_clients_and_zipf_domains(self):
        workload = Workload(config(queries_per_second=20, num_domains=20, zipf_alpha=1.2))
        bins, domains, clients = [0] * 2000, Counter(), Counter()
        while workload.next_time < len(bins):
            when, client, domain = workload.pop()
            bins[int(when)] += 1
            domains[domain] += 1
            clients[client] += 1
        self.assertAlmostEqual(statistics.mean(bins), 20, delta=0.4)
        self.assertAlmostEqual(statistics.pvariance(bins), 20, delta=2)
        normalization = sum(rank ** -1.2 for rank in range(1, 21))
        self.assertAlmostEqual(domains[0] / sum(domains.values()), 1 / normalization, delta=0.012)
        for count in clients.values():
            self.assertAlmostEqual(count / sum(clients.values()), 0.25, delta=0.012)

    def test_uniform_query_distribution(self):
        workload = Workload(config(query_distribution="uniform", num_domains=10))
        counts = Counter(workload.pop()[2] for _ in range(20000))
        for count in counts.values():
            self.assertAlmostEqual(count / 20000, 0.1, delta=0.01)

    def test_regional_client_rankings_are_complete_and_distinct(self):
        generator = HierarchicalZipf(config(num_domains=100, regional_preferences={
            "enabled": True, "shared_rank_count": 8,
        }))
        rankings = [[generator.domain_for_rank(client, rank) for rank in range(100)]
                    for client in range(4)]
        for ranking in rankings:
            self.assertEqual(set(ranking), set(range(100)))
        self.assertNotEqual(rankings[0], rankings[2])
        self.assertNotEqual(rankings[0], rankings[1])

    def test_single_domain_and_extreme_regional_alpha_terminate(self):
        for size in (1, 100):
            generator = HierarchicalZipf(config(num_domains=size, regional_preferences={
                "enabled": True, "region_zipf_alpha": 1000, "client_zipf_alpha": 1001,
                "shared_rank_count": 200,
            }))
            self.assertEqual({generator.domain_for_rank(0, rank) for rank in range(size)}, set(range(size)))

    def test_authority_ttl_assignment_is_identical_between_models(self):
        options = {"num_domains": 10000, "ttl_distribution": {
            "values": [0, 60, 1000], "probabilities": [0.1, 0.7, 0.2],
        }}
        first, second = [ProtocolHarness(model, **options).layers["authority"].authority
                         for model in ("baseline", "ReDNS")]
        self.assertEqual(first.ttls, second.ttls)
        counts = Counter(first.ttls)
        for ttl, probability in zip((0, 60, 1000), (0.1, 0.7, 0.2)):
            self.assertAlmostEqual(counts[ttl] / 10000, probability, delta=0.02)


class ClockTests(unittest.TestCase):
    def test_scaled_monotonic_clock_preserves_fractional_ttl(self):
        clock = RealtimeClock()
        self.assertEqual(clock.now, 0)
        clock.start(1_000_000_000, 0.0001)
        with patch("lib.clock.perf_counter_ns", return_value=1_000_050_000):
            self.assertAlmostEqual(clock.now, 0.5)
            record = AuthoritativeServer([0.75], clock).lookup(0)
            self.assertEqual(record.expires_at, 1.25)
        with patch("lib.clock.perf_counter_ns", return_value=1_000_100_000):
            self.assertAlmostEqual(record.remaining_ttl(clock.now), 0.25)
        clock.freeze(1_000_125_000)
        self.assertTrue(record.is_expired(clock.now))

    def test_manual_clock_rejects_backwards_or_invalid_time(self):
        clock = ManualClock()
        clock.advance_to(1)
        for invalid in (0, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                clock.advance_to(invalid)

    def test_invalid_record_values_are_rejected(self):
        for args in ((-1, 100, 100), (True, 100, 100), (0, -1, 100),
                     (0, 100, -1), (0, float("nan"), 100)):
            with self.assertRaises(ValueError):
                Record(*args)
