import random
import unittest

from sim.clock import SimulationClock
from sim.experiment import validate_config
from sim.preferences import HierarchicalZipf, PreferenceLearner
from src.Re_DNS.Model import ReDNS_Resolver


def preference_config(**updates):
    config = {
        "num_domains": 500,
        "num_clients": 20,
        "num_forwarders": 2,
        "regional_preferences": {
            "enabled": True,
            "region_zipf_alpha": 1.15,
            "client_zipf_alpha": 1.6,
            "shared_rank_count": 32,
            "learning_half_life_seconds": 100.0,
            "max_learned_domains_per_child": 16,
        },
    }
    config.update(updates)
    return validate_config(config)


class PreferenceTests(unittest.TestCase):
    def test_client_rankings_are_bijections_with_regional_overlap(self):
        model = HierarchicalZipf(preference_config())
        for client in (0, 1, 2):
            ranking = [model.domain_for_rank(client, rank) for rank in range(model.size)]
            self.assertEqual(len(set(ranking)), model.size)
        same_region = (set(model.domain_for_rank(0, rank) for rank in range(32)) &
                       set(model.domain_for_rank(2, rank) for rank in range(32)))
        other_region = (set(model.domain_for_rank(0, rank) for rank in range(32)) &
                        set(model.domain_for_rank(1, rank) for rank in range(32)))
        self.assertGreater(len(same_region), len(other_region))

    def test_online_learner_uses_only_observations_and_avoids_known_copy(self):
        learner = PreferenceLearner(100, 8)
        for _ in range(10):
            learner.observe(7, 3, 0)
        learner.observe(8, 3, 0)
        target, score = learner.choose(3, range(7, 9), 60, 0)
        self.assertEqual(target, 7)
        self.assertGreater(score, 0)
        learner.note_copy(7, 3, 100)
        self.assertEqual(learner.choose(3, range(7, 9), 60, 1)[0], 8)
        learner.forget_copy(7, 3, 100)
        self.assertEqual(learner.choose(3, range(7, 9), 60, 1)[0], 7)
        self.assertEqual(learner.choose(99, range(7, 9), 60, 1), (None, 0.0))

    def test_transfer_lifecycle_and_duplicates(self):
        clock = SimulationClock()
        resolver = ReDNS_Resolver(1)
        resolver.clock = clock
        self.assertTrue(resolver.receive_transfer("a", 10, 10))
        original_deadline = resolver.cache["a"]._expires_at
        self.assertFalse(resolver.receive_transfer("a", 100, 100))
        self.assertEqual(resolver.cache["a"]._expires_at, original_deadline)
        self.assertEqual(resolver.stats["transfer_duplicates"], 1)
        self.assertIsNotNone(resolver.check_cache("a", foreground=False))
        self.assertEqual(resolver.stats["transfer_records_used"], 0)
        self.assertIsNotNone(resolver.check_cache("a", foreground=True))
        self.assertEqual(resolver.stats["transfer_records_used"], 1)
        self.assertEqual(resolver.stats["transfer_hits"], 1)

        unused = ReDNS_Resolver(1)
        unused.clock = clock
        unused.receive_transfer("a", 10, 10)
        unused.insert_cache("b", 10, 10)
        self.assertEqual(unused.stats["transfer_unused_evicted"], 1)

    def test_expired_unused_transfer_is_accounted_by_heap(self):
        clock = SimulationClock()
        resolver = ReDNS_Resolver(2)
        resolver.clock = clock
        resolver.receive_transfer("a", 1, 1)
        clock.advance(1)
        resolver.evict_0_TTL()
        self.assertEqual(resolver.stats["transfer_unused_expired"], 1)


if __name__ == "__main__":
    unittest.main()
