"""Synchronous DNS hierarchy with independent, TTL-aware LRU caches."""

from collections import Counter, OrderedDict
from zlib import crc32

if __package__:
    from .Record import Record
    if "." in __package__:
        from ..expiry import ExpiryIndex
    else:
        from expiry import ExpiryIndex
else:  # Allow running Worker.py directly.
    from pathlib import Path
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from expiry import ExpiryIndex
    from Record import Record


class ReDNS_Resolver:
    def __init__(self, cache_size, min_transfer_ttl=30):
        self.cache_size = cache_size
        self.cache = OrderedDict()
        self.clock = None
        self._expiry_index = None
        self.latency_tracker = None
        self.upstream_rtt_ms = 0
        self.min_transfer_ttl = Record._validate_seconds(min_transfer_ttl)
        self.downstream_resolvers = []
        self.preference_learner = None
        self.stats = Counter(
            queries=0, hits=0, misses=0, upstream_queries=0,
            expired_evictions=0, lru_evictions=0,
            records_transferred=0, records_received=0, refresh_queries=0,
            transfers_accepted=0, transfer_duplicates=0,
            transfer_expired_in_transit=0, transfer_hits=0,
            transfer_records_used=0, transfer_unused_expired=0,
            transfer_unused_evicted=0, transfer_unused_replaced=0,
            preference_selected_transfers=0, preference_fallback_transfers=0,
        )

    def _on_cache_remove(self, key, record, reason):
        cohort = getattr(record, "_transfer_stats", None)
        if cohort is not None and not getattr(record, "_transfer_used", False):
            cohort[f"transfer_unused_{reason}"] += 1
    # TTL countdown.
    def countdown(self, seconds=1):
        for record in self.cache.values():
            record.countdown(seconds)

    # Remove expired entries only at the original lazy eviction points.
    def evict_0_TTL(self):
        if self.clock is None:
            evicted_keys = [key for key, record in self.cache.items() if record.is_expired()]
            for key in evicted_keys:
                self._on_cache_remove(key, self.cache[key], "expired")
                del self.cache[key]
            removed = len(evicted_keys)
        else:
            removed = self._expiry_index.evict_expired(self.clock.now) if self._expiry_index else 0
        self.stats["expired_evictions"] += removed
        return bool(removed)

    def add_downstream(self, resolver):
        if resolver.cache_size > 0:
            self.downstream_resolvers.append(resolver)

    def _transfer_evicted(self, record):
        # no downstream resolvers, nothing to do
        if not self.downstream_resolvers:
            return
        # Transfers prepare future requests. Exclude the entire maintenance
        # chain, including upstream cache misses while refreshing a record.
        if self.latency_tracker is None:
            self._store_downstream(record)
        else:
            with self.latency_tracker.background():
                self._store_downstream(record)

    def _store_downstream(self, record):
        # Prefer a downstream that has recently forwarded this name.  The
        # learner sees misses only; it has no access to workload ground truth.
        target = None
        if self.preference_learner is not None:
            domain = int(record.r_name[7:-8])
            ids = [child.downstream_id for child in self.downstream_resolvers]
            chosen, _ = self.preference_learner.choose(
                domain, ids, record.ttl, self.clock.now if self.clock is not None else 0)
            if chosen is not None:
                target = next(child for child in self.downstream_resolvers
                              if child.downstream_id == chosen)
                self.stats["preference_selected_transfers"] += 1
        if target is None:
            target = self.downstream_resolvers[
                crc32(record.r_name.encode("utf-8")) % len(self.downstream_resolvers)
            ]
            self.stats["preference_fallback_transfers"] += 1
        if record.ttl < self.min_transfer_ttl:
            self.stats["refresh_queries"] += 1
            self.stats["upstream_queries"] += 1
            # A refresh reads upstream caches without refilling them, avoiding
            # eviction/refresh cycles when several levels have small caches.
            record = self._fetch_upstream(record.r_name, cache_result=False)
            record.ttl = max(record.ttl, self.min_transfer_ttl)
        accepted = target.receive_transfer(record.r_name, record.ttl, record.auth_ttl)
        if accepted and self.preference_learner is not None:
            cached = target.cache[record.r_name]
            now = self.clock.now if self.clock is not None else 0
            self.preference_learner.note_copy(target.downstream_id, domain,
                                              now + cached.ttl)
        self.stats["records_transferred"] += 1

    def insert_cache(self, key, TTL, auth_TTL, *, expires_at=None):
        r = Record(key, TTL, auth_TTL, clock=self.clock)
        if expires_at is not None and self.clock is not None:
            r._expires_at = expires_at
        # Replacing a key must not evict an unrelated entry.
        replaced = self.cache.pop(key, None)
        if replaced is not None:
            self._on_cache_remove(key, replaced,
                                  "expired" if replaced.is_expired() else "replaced")
        if self.cache_size == 0 or r.is_expired():
            if self._expiry_index is not None:
                self._expiry_index.compact()
            return
        # Evict expired entries first, then evict the least recently used entry if necessary.
        if len(self.cache) >= self.cache_size:
            self.evict_0_TTL()
        evicted = None
        if len(self.cache) >= self.cache_size:
            evicted_key, evicted = self.cache.popitem(last=False)
            self._on_cache_remove(evicted_key, evicted, "evicted")
            self.stats["lru_evictions"] += 1
        # Insert the new record at the end of the cache to mark it as most recently used.
        self.cache[key] = r
        if self.clock is not None:
            if self._expiry_index is None:
                self._expiry_index = ExpiryIndex(self.cache, self._on_cache_remove)
            self._expiry_index.track(key, r)
        if evicted is not None:
            self._transfer_evicted(evicted)

    def receive_transfer(self, key, TTL, auth_TTL, *, expires_at=None):
        """Insert a staged record without replacing a live cached copy."""
        self.stats["records_received"] += 1
        existing = self.cache.get(key)
        if existing is not None and not existing.is_expired():
            self.stats["transfer_duplicates"] += 1
            return False
        deadline = expires_at
        if deadline is not None and self.clock is not None and deadline <= self.clock.now:
            self.stats["transfer_expired_in_transit"] += 1
            return False
        if deadline is None and TTL <= 0:
            self.stats["transfer_expired_in_transit"] += 1
            return False
        self.insert_cache(key, TTL, auth_TTL, expires_at=expires_at)
        record = self.cache.get(key)
        if record is None:
            return False
        record._transfer_stats = self.stats
        record._transfer_used = False
        self.stats["transfers_accepted"] += 1
        return True

    def check_cache(self, key, *, foreground=True):
        record = self.cache.get(key)
        if record is None:
            return None
        # If the record has expired, remove it from the cache and return None.
        if record.is_expired():
            self._on_cache_remove(key, record, "expired")
            del self.cache[key]
            if self._expiry_index is not None:
                self._expiry_index.compact()
            self.stats["expired_evictions"] += 1
            return None
        # Move the accessed record to the end of the cache to mark it as most recently used.
        self.cache.move_to_end(key)
        if foreground and getattr(record, "_transfer_stats", None) is not None:
            self.stats["transfer_hits"] += 1
            if not record._transfer_used:
                record._transfer_used = True
                record._transfer_stats["transfer_records_used"] += 1
        return record.copy()

    def _handle_query(self, query, fetch, *, cache_result=True):
        self.stats["queries"] += 1
        #lookup in cache first, if not found, fetch from upstream resolver and insert into cache
        foreground = not (self.latency_tracker is not None and self.latency_tracker.is_background)
        record = self.check_cache(query, foreground=foreground)
        if record is not None:
            self.stats["hits"] += 1
            return record
        self.stats["misses"] += 1
        self.stats["upstream_queries"] += 1
        if self.latency_tracker is not None:
            self.latency_tracker.add_rtt(self.upstream_rtt_ms)
        # Fetch the record from the upstream resolver.
        record = fetch(query, cache_result=cache_result)
        # Propagate the remaining TTL; a cache hit never renews its lifetime.
        if cache_result:
            self.insert_cache(query, record.ttl, record.auth_ttl)
        return record.copy()

    def handle_query(self, query, *, cache_result=True):
        return self._handle_query(query, self._fetch_upstream, cache_result=cache_result)

    def _fetch_upstream(self, query, *, cache_result=True):
        raise NotImplementedError

    def metrics(self):
        result = dict(self.stats)
        result["hit_rate"] = self.stats["hits"] / self.stats["queries"] if self.stats["queries"] else 0.0
        result["cache_entries"] = len(self.cache)
        result["live_cache_entries"] = sum(not r.is_expired() for r in self.cache.values())
        result["cache_size"] = self.cache_size
        result["transfer_unused_live"] = sum(
            getattr(r, "_transfer_stats", None) is not None
            and not getattr(r, "_transfer_used", False) and not r.is_expired()
            for r in self.cache.values()
        )
        result["transfer_unused_expired_pending"] = sum(
            getattr(r, "_transfer_stats", None) is not None
            and not getattr(r, "_transfer_used", False) and r.is_expired()
            for r in self.cache.values()
        )
        accepted = self.stats["transfers_accepted"]
        result["transfer_use_rate"] = (
            self.stats["transfer_records_used"] / accepted if accepted else 0.0)
        unused_done = sum(self.stats[f"transfer_unused_{reason}"]
                          for reason in ("expired", "evicted", "replaced"))
        result["transfer_waste_rate"] = unused_done / accepted if accepted else 0.0
        return result


class ReDNS_AuthoritativeServer:
    """A fixed zone: every authoritative query returns a fresh original TTL.

    Network delays and root/TLD referrals are abstracted into this lookup.
    Unknown names raise KeyError; negative DNS caching is outside this baseline.
    """

    def __init__(self, records):
        # initialize hosted records
        self.records = {name: Record(name, ttl) for name, ttl in records.items()}
        self.queries = 0

    # return answer
    def handle_query(self, query):
        self.queries += 1
        return self.records[query].copy()


class ReDNS_Forwarder(ReDNS_Resolver):
    def __init__(self, cache_size, pub_server, forwarder_id, min_transfer_ttl=30):
        super().__init__(cache_size, min_transfer_ttl)
        self.pub_server = pub_server
        self.forwarder_id = forwarder_id
        self.downstream_id = forwarder_id
        # add this forwarder to the public server's downstream resolvers
        pub_server.add_downstream(self)

    def forward_query(self, query, pub_server=None, *, cache_result=True):
        server = self.pub_server if pub_server is None else pub_server
        return server.handle_query_from(query, self.forwarder_id, cache_result=cache_result)

    def handle_query_from(self, query, client_id, *, cache_result=True):
        if self.preference_learner is not None and cache_result:
            now = self.clock.now if self.clock is not None else 0
            self.preference_learner.observe(client_id, int(query[7:-8]), now)
        answer = self.handle_query(query, cache_result=cache_result)
        child = next((child for child in self.downstream_resolvers
                      if child.downstream_id == client_id), None)
        if (self.preference_learner is not None and cache_result
                and child is not None and child.cache_size):
            now = self.clock.now if self.clock is not None else 0
            self.preference_learner.note_copy(client_id, int(query[7:-8]), now + answer.ttl)
        return answer

    def _fetch_upstream(self, query, *, cache_result=True):
        return self.forward_query(query, cache_result=cache_result)


class ReDNS_PublicDNSServer(ReDNS_Resolver):
    def __init__(self, cache_size, authoritative_server=None, min_transfer_ttl=30):
        super().__init__(cache_size, min_transfer_ttl)
        self.authoritative_server = authoritative_server

    def _fetch_upstream(self, query, *, cache_result=True):
        return self.resolve_query(query)

    def handle_query_from(self, query, forwarder_id, *, cache_result=True):
        if self.preference_learner is not None and cache_result:
            now = self.clock.now if self.clock is not None else 0
            self.preference_learner.observe(forwarder_id, int(query[7:-8]), now)
        answer = self.handle_query(query, cache_result=cache_result)
        child = next((child for child in self.downstream_resolvers
                      if child.downstream_id == forwarder_id), None)
        if (self.preference_learner is not None and cache_result
                and child is not None and child.cache_size):
            now = self.clock.now if self.clock is not None else 0
            self.preference_learner.note_copy(forwarder_id, int(query[7:-8]), now + answer.ttl)
        return answer

    def do_recursive_resolution(self, query):
        if self.authoritative_server is None:
            raise RuntimeError("PublicDNSServer requires an authoritative_server")
        return self.authoritative_server.handle_query(query)

    def resolve_query(self, query):
        return self.do_recursive_resolution(query)


class ReDNS_Client(ReDNS_Resolver):
    def __init__(self, cache_size, forwarder, client_id=None, min_transfer_ttl=30):
        super().__init__(cache_size, min_transfer_ttl)
        self.forwarder = forwarder
        self.client_id = client_id
        self.downstream_id = client_id
        forwarder.add_downstream(self)

    def send_query(self, query):
        return self.handle_query(query)

    def _fetch_upstream(self, query, *, cache_result=True):
        return self.forwarder.handle_query_from(query, self.client_id, cache_result=cache_result)

