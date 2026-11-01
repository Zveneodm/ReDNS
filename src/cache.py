"""Shared TTL-aware cache behavior and transfer lifecycle accounting."""

from collections import Counter
from dataclasses import dataclass
from typing import Optional

from src.expiry import ExpiryIndex
from src.records import Record
from src.replacement import create_cache


STAT_FIELDS = (
    "queries", "hits", "misses", "upstream_queries",
    "expired_evictions", "lru_evictions", "random_evictions",
    "peer_queries", "peer_hits", "peer_misses", "peer_transfer_hits",
    "successful_reverse_queries",
    "peer_queries_received", "peer_hits_served",
    "cluster_queries", "cluster_hits", "cluster_misses",
    "cluster_forwarder_hits", "cluster_client_hits", "cluster_transfer_hits",
    "cluster_queries_received", "cluster_hits_served", "cluster_misses_served",
    "cluster_cache_hits_served",
    "cluster_child_queries", "cluster_child_hits", "cluster_child_misses",
    "manager_queries", "manager_directory_hits", "manager_directory_misses",
    "manager_updates_sent", "manager_feedback_sent",
    "prefetch_reports_sent", "prefetch_observations_sent",
    "prefetch_tasks_received", "prefetch_started", "prefetch_completed",
    "prefetch_skipped", "prefetch_stored", "prefetch_records_used",
    "records_transferred", "records_received", "transfers_accepted",
    "transfer_duplicates", "transfer_expired_in_transit", "transfers_rejected_full",
    "transfer_hits", "transfer_records_used", "transfer_unused_expired",
    "transfer_unused_evicted", "transfer_unused_replaced",
    "transfer_skipped_known_copies", "transfer_skipped_full_target",
    "directory_stores_sent", "directory_removes_sent",
    "directory_stores_received", "directory_removes_received",
)


def new_stats():
    return Counter(dict.fromkeys(STAT_FIELDS, 0))


@dataclass
class CacheEntry:
    record: Record
    transfer_stats: Optional[Counter] = None
    transfer_used: bool = False
    expiry_version: int = 0
    prefetch_stats: Optional[Counter] = None
    prefetch_used: bool = False


class DNSCache:
    """Ordinary LRU/random caching with optional cache-change callbacks.

    The simulator supplies a clock and transport callbacks. This class never
    sends messages or reads configuration files. Every node owns its mapping;
    only immutable answer metadata can be reused between local objects.
    """

    def __init__(self, capacity, clock, *, replacement_policy="lru", replacement_seed=None):
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 0:
            raise ValueError("cache capacity must be a non-negative integer")
        self.capacity = capacity
        self.clock = clock
        self.replacement_policy = replacement_policy
        self.cache = create_cache(replacement_policy, replacement_seed)
        self.expiry = ExpiryIndex(self.cache)
        self.stats = new_stats()
        self.on_store = None
        self.on_remove = None
        self.on_evict = None

    def _removed(self, entry, reason):
        if entry.transfer_stats is not None and not entry.transfer_used:
            entry.transfer_stats[f"transfer_unused_{reason}"] += 1
        if self.on_remove is not None:
            self.on_remove(entry.record, reason)

    def _stored(self, record):
        if self.on_store is not None:
            self.on_store(record)

    def purge_expired(self):
        for domain in self.expiry.expired_domains(self.clock.now):
            self._removed(self.cache.pop(domain), "expired")
            self.stats["expired_evictions"] += 1

    def check_cache(self, domain, *, foreground=True):
        entry = self.cache.get(domain)
        if entry is None:
            return None
        if entry.record.is_expired(self.clock.now):
            del self.cache[domain]
            self._removed(entry, "expired")
            self.stats["expired_evictions"] += 1
            self.expiry.compact()
            return None
        self.cache.move_to_end(domain)
        if foreground and entry.prefetch_stats is not None and not entry.prefetch_used:
            entry.prefetch_used = True
            entry.prefetch_stats["prefetch_records_used"] += 1
        if foreground and entry.transfer_stats is not None:
            self.stats["transfer_hits"] += 1
            if not entry.transfer_used:
                entry.transfer_used = True
                entry.transfer_stats["transfer_records_used"] += 1
        return entry.record

    def insert_cache(self, record):
        """Normal response refill may replace a live capacity victim."""
        previous = self.cache.pop(record.domain, None)
        if previous is not None:
            reason = "expired" if previous.record.is_expired(self.clock.now) else "replaced"
            self._removed(previous, reason)
        if not self.capacity or record.is_expired(self.clock.now):
            self.expiry.compact()
            return False
        if len(self.cache) >= self.capacity:
            self.purge_expired()
        evicted = None
        if len(self.cache) >= self.capacity:
            _, evicted = self.cache.popitem(last=False)
            self._removed(evicted, "evicted")
            self.stats[f"{self.replacement_policy}_evictions"] += 1
        entry = CacheEntry(record)
        self.cache[record.domain] = entry
        self.expiry.track(record.domain, entry)
        self._stored(record)
        if evicted is not None and self.on_evict is not None:
            self.on_evict(evicted.record)
        return True

    def receive_transfer(self, record):
        """Accept only into spare capacity; keep the incoming absolute TTL."""
        self.stats["records_received"] += 1
        existing = self.cache.get(record.domain)
        if existing is not None and not existing.record.is_expired(self.clock.now):
            self.stats["transfer_duplicates"] += 1
            # Repair the sender's hint with the actual cached deadline.
            self._stored(existing.record)
            return False
        if record.is_expired(self.clock.now):
            self.stats["transfer_expired_in_transit"] += 1
            return False
        if len(self.cache) >= self.capacity:
            self.purge_expired()
        if len(self.cache) >= self.capacity:
            self.stats["transfers_rejected_full"] += 1
            return False
        if not self.insert_cache(record):
            return False
        self.cache[record.domain].transfer_stats = self.stats
        self.stats["transfers_accepted"] += 1
        return True

    def metrics(self):
        result = dict(self.stats)
        result.update(
            cache_size=self.capacity,
            cache_entries=len(self.cache),
            live_cache_entries=sum(not entry.record.is_expired(self.clock.now)
                                   for entry in self.cache.values()),
        )
        result["hit_rate"] = result["hits"] / result["queries"] if result["queries"] else 0.0
        for reason in ("live", "expired_pending"):
            result[f"transfer_unused_{reason}"] = 0
        for entry in self.cache.values():
            # Gauges belong to the acceptance cohort, even across warm-up.
            if entry.transfer_stats is self.stats and not entry.transfer_used:
                reason = "expired_pending" if entry.record.is_expired(self.clock.now) else "live"
                result[f"transfer_unused_{reason}"] += 1
        accepted = result["transfers_accepted"]
        result["transfer_use_rate"] = result["transfer_records_used"] / accepted if accepted else 0.0
        wasted = sum(result[f"transfer_unused_{reason}"]
                     for reason in ("expired", "evicted", "replaced"))
        result["transfer_waste_rate"] = wasted / accepted if accepted else 0.0
        return result
