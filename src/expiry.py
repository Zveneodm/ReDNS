"""Lazy expiry index with bounded stale heap entries."""

from heapq import heapify, heappop, heappush


class ExpiryIndex:
    """Versions distinguish replacements with the same domain and deadline."""

    def __init__(self, cache):
        self.cache = cache
        self.heap = []
        self._version = 0

    def track(self, domain, entry):
        self._version += 1
        entry.expiry_version = self._version
        heappush(self.heap, (entry.record.expires_at, self._version, domain))
        self.compact()

    def expired_domains(self, now):
        while self.heap and self.heap[0][0] <= now:
            _, version, domain = heappop(self.heap)
            entry = self.cache.get(domain)
            if entry is not None and entry.expiry_version == version:
                yield domain
        self.compact()

    def compact(self):
        # Compaction drops stale heap items, without removing cached records.
        if len(self.heap) > max(64, 2 * len(self.cache)):
            self.heap = [(entry.record.expires_at, entry.expiry_version, domain)
                         for domain, entry in self.cache.items()]
            heapify(self.heap)
