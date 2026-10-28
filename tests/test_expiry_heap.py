from collections import OrderedDict
import unittest

from sim.clock import SimulationClock
from src.baseline.Model import Resolver
from src.Re_DNS.Model import ReDNS_Resolver


class NoScanCache(OrderedDict):
    def items(self):
        raise AssertionError("unexpected full cache scan")

    def values(self):
        raise AssertionError("unexpected full cache scan")


class ExpiryHeapTests(unittest.TestCase):
    def resolvers(self, size):
        for model in (Resolver, ReDNS_Resolver):
            resolver = model(size)
            resolver.clock = SimulationClock()
            yield resolver

    def test_replacement_ignores_old_deadline(self):
        for resolver in self.resolvers(2):
            with self.subTest(model=type(resolver).__name__):
                resolver.insert_cache("a", 1, 100)
                resolver.insert_cache("a", 100, 100)
                resolver.clock.advance()
                self.assertFalse(resolver.evict_0_TTL())
                self.assertEqual(resolver.cache["a"].ttl, 99)
                self.assertEqual(resolver.stats["expired_evictions"], 0)
                resolver.insert_cache("a", 1, 100)
                resolver.clock.advance()
                self.assertTrue(resolver.evict_0_TTL())
                resolver.clock.advance(100)
                self.assertFalse(resolver.evict_0_TTL())
                self.assertEqual(resolver.stats["expired_evictions"], 1)

    def test_lru_eviction_then_reinsertion_is_not_expired_by_old_entry(self):
        for resolver in self.resolvers(2):
            with self.subTest(model=type(resolver).__name__):
                resolver.insert_cache("a", 1, 100)
                resolver.insert_cache("b", 100, 100)
                resolver.insert_cache("c", 100, 100)
                resolver.insert_cache("a", 100, 100)
                resolver.clock.advance()
                self.assertFalse(resolver.evict_0_TTL())
                self.assertEqual(list(resolver.cache), ["c", "a"])
                self.assertEqual(resolver.stats["lru_evictions"], 2)
                self.assertEqual(resolver.stats["expired_evictions"], 0)

    def test_query_removal_is_not_counted_twice(self):
        for resolver in self.resolvers(2):
            with self.subTest(model=type(resolver).__name__):
                resolver.insert_cache("a", 1, 100)
                resolver.insert_cache("b", 2, 100)
                resolver.clock.advance()
                self.assertIsNone(resolver.check_cache("a"))
                resolver.clock.advance()
                self.assertTrue(resolver.evict_0_TTL())
                self.assertEqual(resolver.stats["expired_evictions"], 2)
                self.assertFalse(resolver.evict_0_TTL())

    def test_all_expired_entries_are_removed_before_lru_eviction(self):
        for resolver in self.resolvers(5):
            with self.subTest(model=type(resolver).__name__):
                for name in ("a", "b", "c"):
                    resolver.insert_cache(name, 1, 100)
                for name in ("d", "e"):
                    resolver.insert_cache(name, 100, 100)
                resolver.clock.advance()
                resolver.insert_cache("f", 100, 100)
                self.assertEqual(list(resolver.cache), ["d", "e", "f"])
                self.assertEqual(resolver.stats["expired_evictions"], 3)
                self.assertEqual(resolver.stats["lru_evictions"], 0)

    def test_direct_ttl_changes_update_the_heap(self):
        for resolver in self.resolvers(2):
            with self.subTest(model=type(resolver).__name__):
                resolver.insert_cache("a", 100, 100)
                resolver.insert_cache("b", 1, 100)
                resolver.cache["a"].ttl = 1
                resolver.cache["b"].TTL = 100
                resolver.clock.advance()
                self.assertTrue(resolver.evict_0_TTL())
                self.assertEqual(list(resolver.cache), ["b"])
                resolver.cache["b"].countdown(99)
                self.assertTrue(resolver.evict_0_TTL())
                self.assertEqual(resolver.stats["expired_evictions"], 2)

    def test_changes_to_removed_records_and_answers_do_not_update_index(self):
        for resolver in self.resolvers(2):
            with self.subTest(model=type(resolver).__name__):
                resolver.insert_cache("a", 100, 100)
                old = resolver.cache["a"]
                answer = resolver.check_cache("a")
                resolver.insert_cache("a", 100, 100)
                entries = list(resolver._expiry_index.heap)
                old.ttl = 0
                answer.ttl = 0
                self.assertEqual(resolver._expiry_index.heap, entries)
                self.assertFalse(resolver.evict_0_TTL())
                self.assertEqual(resolver.cache["a"].ttl, 100)

    def test_compaction_bounds_heap_without_eagerly_expiring_records(self):
        for resolver in self.resolvers(2):
            with self.subTest(model=type(resolver).__name__):
                resolver.insert_cache("a", 1, 100)
                resolver.insert_cache("b", 100, 100)
                resolver.clock.advance()
                for _ in range(300):
                    resolver.insert_cache("b", 100, 100)
                self.assertLessEqual(len(resolver._expiry_index.heap), 64)
                self.assertEqual(list(resolver.cache), ["a", "b"])
                self.assertEqual(resolver.stats["expired_evictions"], 0)
                self.assertTrue(resolver.evict_0_TTL())
                self.assertEqual(resolver.stats["expired_evictions"], 1)
                for i in range(300):
                    resolver.insert_cache(str(i), 100, 100)
                self.assertLessEqual(len(resolver._expiry_index.heap), 64)
                self.assertEqual(list(resolver.cache), ["298", "299"])

    def test_full_cache_without_expired_entries_is_not_scanned(self):
        for resolver in self.resolvers(1000):
            with self.subTest(model=type(resolver).__name__):
                resolver.cache = NoScanCache()
                for i in range(1005):
                    resolver.insert_cache(str(i), 100, 100)
                self.assertFalse(resolver.evict_0_TTL())
                self.assertEqual(resolver.stats["lru_evictions"], 5)
                self.assertEqual(resolver.stats["expired_evictions"], 0)

    def test_disabled_and_standalone_caches_remain_supported(self):
        for resolver in self.resolvers(0):
            with self.subTest(model=type(resolver).__name__):
                resolver.insert_cache("a", 100, 100)
                self.assertEqual(len(resolver.cache), 0)
                self.assertIsNone(resolver._expiry_index)
                self.assertFalse(resolver.evict_0_TTL())
        for model in (Resolver, ReDNS_Resolver):
            resolver = model(1)
            resolver.insert_cache("a", 1, 100)
            resolver.countdown()
            self.assertTrue(resolver.evict_0_TTL())
            self.assertEqual(resolver.stats["expired_evictions"], 1)


if __name__ == "__main__":
    unittest.main()
