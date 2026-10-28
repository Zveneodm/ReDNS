from collections import Counter
import unittest

from sim.experiment import PROJECT_ROOT, load_config, validate_config
from sim.run_baseline import DeterministicSimulation as BaselineSimulation
from sim.run_ReDNS import DeterministicSimulation as ReDNSSimulation


class TTLDistributionTests(unittest.TestCase):
    def config(self, **overrides):
        return {
            "duration_seconds": 2, "queries_per_second": 2,
            "num_clients": 1, "num_forwarders": 1, "num_domains": 10000,
            "ttl_distribution": {
                "values": [20, 60, 300, 900, 3600, 86400, 172800],
                "probabilities": [0.05, 0.35, 0.20, 0.20, 0.14, 0.05, 0.01],
            },
            **overrides,
        }

    def test_sampled_probabilities_and_matching_models(self):
        config = self.config()
        baseline = BaselineSimulation(config)
        redns = ReDNSSimulation(config)
        ttls = [record.auth_ttl for record in baseline.authority.records.values()]
        self.assertEqual(ttls, [record.auth_ttl for record in redns.authority.records.values()])
        repeated = BaselineSimulation(config)
        self.assertEqual(ttls, [record.auth_ttl for record in repeated.authority.records.values()])
        counts = Counter(ttls)
        distribution = config["ttl_distribution"]
        self.assertEqual(set(counts), set(distribution["values"]))
        for ttl, probability in zip(distribution["values"], distribution["probabilities"]):
            self.assertAlmostEqual(counts[ttl] / len(ttls), probability, delta=0.02)

    def test_zero_probability_and_distribution_precedence(self):
        simulation = BaselineSimulation(self.config(
            num_domains=5, ttl_range=[999, 999],
            ttl_distribution={"values": [0, 60, 300], "probabilities": [0, 1, 0]},
        ))
        for name, record in simulation.authority.records.items():
            self.assertEqual(record.auth_ttl, 60)
            for _ in range(2):
                self.assertEqual(simulation.authority.handle_query(name).ttl, 60)

    def test_zero_ttl_is_supported(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            with self.subTest(model=model.experiment_name):
                result = model(self.config(
                    num_domains=1, ttl_distribution={"values": [0], "probabilities": [1]},
                )).run()
                self.assertEqual(result["authoritative_queries"], 4)
                self.assertEqual(result["request_latencies_ms"], [61] * 4)

    def test_ttl_sampling_does_not_change_query_randomness(self):
        first = BaselineSimulation(self.config(num_domains=100))
        second = BaselineSimulation(self.config(
            num_domains=100, ttl_distribution={"values": [900], "probabilities": [1]},
        ))
        self.assertEqual([first._choose_domain() for _ in range(30)],
                         [second._choose_domain() for _ in range(30)])

    def test_warm_up_keeps_cache_and_excludes_measurements(self):
        for model in (BaselineSimulation, ReDNSSimulation):
            with self.subTest(model=model.experiment_name):
                simulation = model(self.config(
                    warm_up_seconds=1, num_domains=1,
                    ttl_distribution={"values": [60], "probabilities": [1]},
                ))
                result = simulation.run()
                self.assertEqual(result["warm_up_elapsed_seconds"], 1)
                self.assertEqual(result["elapsed_seconds"], 2)
                self.assertEqual(result["total_queries"], 4)
                self.assertEqual(result["client"]["hits"], 4)
                self.assertEqual(result["authoritative_queries"], 0)
                self.assertEqual(result["request_latencies_ms"], [0] * 4)
                self.assertEqual(next(iter(simulation.clients[0].cache.values())).ttl, 57)
                self.assertEqual(simulation.run(), result)

    def test_invalid_distributions(self):
        invalid = (
            [], {}, {"values": [60]},
            {"values": [], "probabilities": []},
            {"values": [True], "probabilities": [1]},
            {"values": [-1], "probabilities": [1]},
            {"values": [1.5], "probabilities": [1]},
            {"values": [60], "probabilities": [0.5]},
            {"values": [60], "probabilities": [True]},
            {"values": [60], "probabilities": [float("nan")]},
            {"values": [20, 60], "probabilities": [1]},
            {"values": [20, 60], "probabilities": [-0.5, 1.5]},
        )
        for distribution in invalid:
            with self.subTest(distribution=distribution), self.assertRaises(ValueError):
                validate_config({"ttl_distribution": distribution})

    def test_project_config_loads(self):
        config = load_config(PROJECT_ROOT / "config.json")
        self.assertEqual(config["ttl_distribution"]["values"],
                         [20, 60, 300, 900, 3600, 86400, 172800])


if __name__ == "__main__":
    unittest.main()
