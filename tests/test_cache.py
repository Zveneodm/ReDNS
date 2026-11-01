from collections import Counter, OrderedDict
from dataclasses import FrozenInstanceError
import random
import unittest

from lib.clock import ManualClock
from src.cache import DNSCache, new_stats
from src.models import AuthoritativeServer, BaselineForwarder, ReDNSForwarder
from src.records import Record
from src.replacement import RandomCache, replacement_seed


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.clock = ManualClock()

    def test_absolute_ttl_and_immutable_answers(self):
        record = Record(1, 50, 100)
        first, second = [DNSCache(2, self.clock) for _ in range(2)]
        first.insert_cache(record)
        self.clock.advance_to(20)
        second.insert_cache(first.check_cache(1))
        self.assertEqual(second.check_cache(1).remaining_ttl(self.clock.now), 30)
        self.assertEqual(second.check_cache(1).auth_ttl, 100)
        with self.assertRaises(FrozenInstanceError):
            record.expires_at = 500
        self.clock.advance_to(50)
        self.assertIsNone(first.check_cache(1))
        self.assertIsNone(second.check_cache(1))

    def test_authority_restarts_original_ttl(self):
        authority = AuthoritativeServer([30, 0], self.clock)
        self.assertEqual(authority.lookup(0).expires_at, 30)
        self.clock.advance_to(20)
        self.assertEqual(authority.lookup(0).expires_at, 50)
        self.assertEqual(authority.lookup(1).remaining_ttl(20), 0)
        self.assertEqual(authority.queries, 3)

    def test_lru_matches_independent_ttl_reference(self):
        for model in (BaselineForwarder, ReDNSForwarder):
            with self.subTest(model=model.__name__):
                clock = ManualClock()
                node = model(3, clock)
                reference = OrderedDict()
                rng = random.Random(42)
                for _ in range(1000):
                    clock.advance_to(clock.now + rng.random())
                    domain = rng.randrange(10)
                    expected = reference.get(domain)
                    if expected is not None and expected <= clock.now:
                        del reference[domain]
                        expected = None
                    actual = node.check_cache(domain)
                    self.assertEqual(actual is not None, expected is not None)
                    if expected is not None:
                        reference.move_to_end(domain)
                    else:
                        deadline = clock.now + rng.randrange(5)
                        node.insert_cache(Record(domain, deadline, 4))
                        if deadline > clock.now:
                            if len(reference) >= 3:
                                reference = OrderedDict((k, v) for k, v in reference.items()
                                                        if v > clock.now)
                            if len(reference) >= 3:
                                reference.popitem(last=False)
                            reference[domain] = deadline
                    self.assertEqual(list(node.cache), list(reference))

    def test_expired_entries_are_removed_before_live_victims(self):
        for policy in ("lru", "random"):
            self.clock = ManualClock()
            cache = DNSCache(3, self.clock, replacement_policy=policy)
            for domain, expiry in ((0, 1), (1, 100), (2, 1)):
                cache.insert_cache(Record(domain, expiry, 100))
            self.clock.advance_to(1)
            cache.insert_cache(Record(3, 100, 100))
            self.assertEqual(set(cache.cache), {1, 3})
            self.assertEqual(cache.stats["expired_evictions"], 2)
            self.assertEqual(cache.stats[f"{policy}_evictions"], 0)

    def test_replacing_same_domain_does_not_evict_unrelated_record(self):
        cache = DNSCache(2, self.clock)
        cache.insert_cache(Record(0, 10, 10))
        cache.insert_cache(Record(1, 10, 10))
        cache.insert_cache(Record(0, 20, 20))
        self.assertEqual(list(cache.cache), [1, 0])
        self.assertEqual(cache.stats["lru_evictions"], 0)

    def test_zero_capacity_and_zero_ttl_do_not_cache(self):
        cache = DNSCache(0, self.clock)
        self.assertFalse(cache.insert_cache(Record(1, 100, 100)))
        cache = DNSCache(1, self.clock)
        cache.insert_cache(Record(0, 100, 100))
        self.assertFalse(cache.insert_cache(Record(1, 0, 0)))
        self.assertEqual(set(cache.cache), {0})

    def test_heap_versions_and_compaction_keep_new_replacements(self):
        cache = DNSCache(2, self.clock)
        for deadline in range(1, 300):
            cache.insert_cache(Record(1, deadline, 300))
        self.assertLessEqual(len(cache.expiry.heap), 64)
        self.clock.advance_to(200)
        cache.purge_expired()
        self.assertEqual(cache.check_cache(1).expires_at, 299)
        self.clock.advance_to(299)
        cache.purge_expired()
        self.assertFalse(cache.cache)

    def test_store_remove_and_eviction_callbacks_follow_actual_changes(self):
        cache = DNSCache(1, self.clock)
        events = []
        cache.on_store = lambda r: events.append(("store", r.domain))
        cache.on_remove = lambda r, reason: events.append((reason, r.domain))
        cache.on_evict = lambda r: events.append(("transfer", r.domain))
        cache.insert_cache(Record(0, 10, 10))
        cache.insert_cache(Record(0, 20, 20))
        cache.insert_cache(Record(1, 30, 30))
        self.clock.advance_to(30)
        cache.check_cache(1)
        self.assertEqual(events, [("store", 0), ("replaced", 0), ("store", 0),
                                  ("evicted", 0), ("store", 1), ("transfer", 0), ("expired", 1)])

    def test_transfer_rejects_full_cache_and_preserves_live_records(self):
        for policy in ("lru", "random"):
            cache = DNSCache(1, self.clock, replacement_policy=policy)
            cache.insert_cache(Record(0, 100, 100))
            self.assertFalse(cache.receive_transfer(Record(1, 200, 200)))
            self.assertEqual(set(cache.cache), {0})
            self.assertEqual(cache.stats["transfers_rejected_full"], 1)
            self.assertEqual(cache.stats[f"{policy}_evictions"], 0)

    def test_transfer_can_use_expired_slot_without_extending_ttl(self):
        cache = DNSCache(1, self.clock)
        cache.insert_cache(Record(0, 1, 100))
        self.clock.advance_to(2)
        self.assertTrue(cache.receive_transfer(Record(1, 3, 100)))
        self.assertEqual(cache.check_cache(1).remaining_ttl(2), 1)
        self.clock.advance_to(3)
        self.assertIsNone(cache.check_cache(1))

    def test_duplicate_transfer_advertises_actual_deadline(self):
        cache = DNSCache(1, self.clock)
        cache.insert_cache(Record(0, 5, 100))
        notices = []
        cache.on_store = notices.append
        self.assertFalse(cache.receive_transfer(Record(0, 100, 100)))
        self.assertEqual(notices[0].expires_at, 5)
        self.assertEqual(cache.stats["transfer_duplicates"], 1)

    def test_expired_transfer_is_rejected_without_refresh(self):
        cache = DNSCache(1, self.clock)
        self.assertFalse(cache.receive_transfer(Record(0, 0, 100)))
        self.assertEqual(cache.stats["transfer_expired_in_transit"], 1)
        self.assertFalse(cache.cache)

    def test_transfer_use_is_counted_once_per_accepted_record(self):
        cache = DNSCache(1, self.clock)
        cache.receive_transfer(Record(0, 100, 100))
        cache.check_cache(0, foreground=False)
        self.assertEqual(cache.stats["transfer_records_used"], 0)
        for _ in range(3):
            cache.check_cache(0)
        self.assertEqual(cache.stats["transfer_hits"], 3)
        self.assertEqual(cache.stats["transfer_records_used"], 1)

    def test_warm_transfer_cohort_does_not_pollute_formal_usage(self):
        cache = DNSCache(1, self.clock)
        cache.receive_transfer(Record(0, 100, 100))
        warm = cache.stats
        cache.stats = new_stats()
        cache.check_cache(0)
        self.assertEqual(cache.stats["transfer_hits"], 1)
        self.assertEqual(cache.stats["transfer_records_used"], 0)
        self.assertEqual(warm["transfer_records_used"], 1)
        self.assertEqual(cache.metrics()["transfer_unused_live"], 0)

    def test_transfer_cohort_partitions_unused_outcomes(self):
        cache = DNSCache(3, self.clock)
        for domain, deadline in ((0, 1), (1, 100), (2, 100)):
            cache.receive_transfer(Record(domain, deadline, 100))
        self.clock.advance_to(2)
        cache.check_cache(0)
        cache.insert_cache(Record(1, 200, 200))
        self.assertEqual(cache.metrics()["transfer_unused_expired"], 1)
        self.assertEqual(cache.metrics()["transfer_unused_replaced"], 1)
        self.assertEqual(cache.metrics()["transfer_unused_live"], 1)


class ReplacementTests(unittest.TestCase):
    def test_random_replacement_is_uniform_and_reproducible(self):
        counts = Counter()
        for seed in range(1000):
            cache = RandomCache(seed)
            for domain in range(4):
                cache[domain] = domain
            counts[cache.popitem()[0]] += 1
        for count in counts.values():
            self.assertAlmostEqual(count / 1000, 0.25, delta=0.05)
        first, second = RandomCache(42), RandomCache(42)
        for domain in range(100):
            first[domain] = second[domain] = domain
            if len(first) > 4:
                self.assertEqual(first.popitem(), second.popitem())

    def test_random_replacement_can_remove_mru(self):
        cache = DNSCache(2, ManualClock(), replacement_policy="random", replacement_seed=0)
        for domain in (0, 1):
            cache.insert_cache(Record(domain, 100, 100))
        cache.check_cache(1)
        cache.insert_cache(Record(2, 100, 100))
        self.assertEqual(set(cache.cache), {0, 2})

    def test_random_dense_index_survives_deletions(self):
        cache = RandomCache(42)
        for domain in range(100):
            cache[domain] = domain
        for domain in range(0, 100, 2):
            del cache[domain]
        removed = {cache.popitem()[0] for _ in range(50)}
        self.assertEqual(removed, set(range(1, 100, 2)))
        self.assertFalse(cache)

    def test_node_replacement_streams_are_independent(self):
        seeds = {replacement_seed(42, role, node) for role in ("client", "forwarder")
                 for node in range(10)}
        self.assertEqual(len(seeds), 20)
