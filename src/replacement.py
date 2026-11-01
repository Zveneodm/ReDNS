"""Cache replacement policies shared by baseline and ReDNS."""

from collections import OrderedDict
from collections.abc import MutableMapping
import random


def validate_replacement_policy(policy):
    if policy not in ("lru", "random"):
        raise ValueError("replacement_policy must be 'lru' or 'random'")
    return policy


def replacement_seed(seed, role, node_id):
    """Stable per-cache streams, separate from traffic, arrivals and TTLs."""
    return f"{seed}:replacement:{role}:{node_id}"


class RandomCache(MutableMapping):
    """Mapping with uniform O(1) random removal, including after TTL cleanup.

    A dense key array and key positions avoid copying/scanning a full cache
    on every eviction. Hits do not influence the victim selection.
    """

    def __init__(self, seed=None):
        self._records = {}
        self._keys = []
        self._positions = {}
        self._rng = random.Random(seed)

    def __getitem__(self, key):
        return self._records[key]

    def __setitem__(self, key, value):
        if key not in self._records:
            self._positions[key] = len(self._keys)
            self._keys.append(key)
        self._records[key] = value

    def __delitem__(self, key):
        position = self._positions.pop(key)
        last = self._keys.pop()
        if position < len(self._keys):
            self._keys[position] = last
            self._positions[last] = position
        del self._records[key]

    def __iter__(self):
        return iter(self._records)

    def __len__(self):
        return len(self._records)

    def move_to_end(self, key):
        # Resolver hits update recency only for LRU caches.
        if key not in self._records:
            raise KeyError(key)

    def popitem(self, last=False):
        """Remove a random victim; the LRU compatibility flag is ignored."""
        if not self._keys:
            raise KeyError("cache is empty")
        key = self._rng.choice(self._keys)
        return key, self.pop(key)


def create_cache(policy="lru", seed=None):
    validate_replacement_policy(policy)
    return OrderedDict() if policy == "lru" else RandomCache(seed)
