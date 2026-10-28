"""Versioned expiry heap shared by the two DNS cache implementations."""

from heapq import heapify, heappop, heappush
from weakref import ref


class ExpiryIndex:
    def __init__(self, cache, on_remove=None):
        self.cache = cache
        self.on_remove = on_remove
        self.heap = []
        self._version = 0
        # Cached records can notify TTL changes without keeping this index alive.
        self._reference = ref(self)

    def track(self, key, record):
        record._expiry_key = key
        record._expiry_ref = self._reference
        self.update(record)

    def update(self, record):
        key = record._expiry_key
        # An evicted or replaced record may still be held by a caller.
        if self.cache.get(key) is not record:
            return
        self._version += 1
        record._expiry_version = self._version
        heappush(self.heap, (record._expires_at, self._version, key))
        self.compact()

    def evict_expired(self, now):
        removed = 0
        while self.heap and self.heap[0][0] <= now:
            _, version, key = heappop(self.heap)
            record = self.cache.get(key)
            if record is not None and record._expiry_version == version:
                if self.on_remove is not None:
                    self.on_remove(key, record, "expired")
                del self.cache[key]
                removed += 1
        self.compact()
        return removed

    def compact(self):
        # Replacements and removals leave stale entries. Rebuild occasionally,
        # retaining expired cache records until the normal lazy eviction point.
        if len(self.heap) > max(64, 2 * len(self.cache)):
            self.heap = [
                (record._expires_at, record._expiry_version, key)
                for key, record in self.cache.items()
            ]
            heapify(self.heap)
