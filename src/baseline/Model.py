"""Synchronous DNS hierarchy with independent, TTL-aware LRU caches."""

from collections import Counter, OrderedDict

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


class Resolver:
    def __init__(self, cache_size):
        self.cache_size = cache_size
        self.cache = OrderedDict()
        self.clock = None
        self._expiry_index = None
        self.latency_tracker = None
        self.upstream_rtt_ms = 0
        self.stats = Counter(
            queries=0, hits=0, misses=0, upstream_queries=0,
            expired_evictions=0, lru_evictions=0,
        )
    # TTL countdown.
    def countdown(self, seconds=1):
        for record in self.cache.values():
            record.countdown(seconds)

    # Remove expired entries only at the original lazy eviction points.
    def evict_0_TTL(self):
        if self.clock is None:
            evicted_keys = [key for key, record in self.cache.items() if record.is_expired()]
            for key in evicted_keys:
                del self.cache[key]
            removed = len(evicted_keys)
        else:
            removed = self._expiry_index.evict_expired(self.clock.now) if self._expiry_index else 0
        self.stats["expired_evictions"] += removed
        return bool(removed)

    def insert_cache(self, key, TTL, auth_TTL, *, expires_at=None):
        r = Record(key, TTL, auth_TTL, clock=self.clock)
        if expires_at is not None and self.clock is not None:
            r._expires_at = expires_at
        # Replacing a key must not evict an unrelated entry.
        self.cache.pop(key, None)
        if self.cache_size == 0 or r.is_expired():
            if self._expiry_index is not None:
                self._expiry_index.compact()
            return
        # Evict expired entries first, then evict the least recently used entry if necessary.
        if len(self.cache) >= self.cache_size:
            self.evict_0_TTL()
        if len(self.cache) >= self.cache_size:
            self.cache.popitem(last=False)
            self.stats["lru_evictions"] += 1
        # Insert the new record at the end of the cache to mark it as most recently used.
        self.cache[key] = r
        if self.clock is not None:
            if self._expiry_index is None:
                self._expiry_index = ExpiryIndex(self.cache)
            self._expiry_index.track(key, r)

    def check_cache(self, key):
        record = self.cache.get(key)
        if record is None:
            return None
        # If the record has expired, remove it from the cache and return None.
        if record.is_expired():
            del self.cache[key]
            if self._expiry_index is not None:
                self._expiry_index.compact()
            self.stats["expired_evictions"] += 1
            return None
        # Move the accessed record to the end of the cache to mark it as most recently used.
        self.cache.move_to_end(key)
        return record.copy()

    def _handle_query(self, query, fetch):
        self.stats["queries"] += 1
        #lookup in cache first, if not found, fetch from upstream resolver and insert into cache
        record = self.check_cache(query)
        if record is not None:
            self.stats["hits"] += 1
            return record
        self.stats["misses"] += 1
        self.stats["upstream_queries"] += 1
        if self.latency_tracker is not None:
            self.latency_tracker.add_rtt(self.upstream_rtt_ms)
        # Fetch the record from the upstream resolver.
        record = fetch(query)
        # Propagate the remaining TTL; a cache hit never renews its lifetime.
        self.insert_cache(query, record.ttl, record.auth_ttl)
        return record.copy()

    def handle_query(self, query):
        raise NotImplementedError

    def metrics(self):
        result = dict(self.stats)
        result["hit_rate"] = self.stats["hits"] / self.stats["queries"] if self.stats["queries"] else 0.0
        result["cache_entries"] = len(self.cache)
        result["live_cache_entries"] = sum(not r.is_expired() for r in self.cache.values())
        result["cache_size"] = self.cache_size
        return result


class AuthoritativeServer:
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


class Forwarder(Resolver):
    def __init__(self, cache_size, pub_server, forwarder_id):
        super().__init__(cache_size)
        self.pub_server = pub_server
        self.forwarder_id = forwarder_id

    def forward_query(self, query, pub_server=None):
        server = self.pub_server if pub_server is None else pub_server
        return server.handle_query(query)

    def handle_query(self, query):
        return self._handle_query(query, self.forward_query)


class PublicDNSServer(Resolver):
    def __init__(self, cache_size, authoritative_server=None):
        super().__init__(cache_size)
        self.authoritative_server = authoritative_server

    def handle_query(self, query):
        return self._handle_query(query, self.resolve_query)

    def do_recursive_resolution(self, query):
        if self.authoritative_server is None:
            raise RuntimeError("PublicDNSServer requires an authoritative_server")
        return self.authoritative_server.handle_query(query)

    def resolve_query(self, query):
        return self.do_recursive_resolution(query)


class Client(Resolver):
    def __init__(self, cache_size, forwarder, client_id=None):
        super().__init__(cache_size)
        self.forwarder = forwarder
        self.client_id = client_id

    def send_query(self, query):
        return self._handle_query(query, self.forwarder.handle_query)

    def handle_query(self, query):
        return self.send_query(query)

