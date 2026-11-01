"""Synthetic per-client Zipf demand with shared regional hotspots."""

from array import array
from bisect import bisect_right
from math import gcd
import random


def _cdf(size, alpha):
    values = array("d")
    total = 0.0
    for rank in range(1, size + 1):
        total += rank ** -alpha
        values.append(total)
    return values


def _coprime_multiplier(value, modulus):
    if modulus <= 1:
        return 0
    value = value % modulus or 1
    while gcd(value, modulus) != 1:
        value += 1
    return value


class HierarchicalZipf:
    """Exact per-client Zipf ranks with region-biased independent rankings.

    A client's leading ranks are sampled without replacement from its region's
    Zipf prior.  Remaining ranks form a deterministic bijection over all other
    domains, so every client still has a full, duplicate-free ranking.
    """

    def __init__(self, config):
        options = config["regional_preferences"]
        self.size = config["num_domains"]
        self.regions = config["num_forwarders"]
        self.seed = config["seed"]
        self.head_size = min(options["shared_rank_count"], self.size)
        self.client_cdf = _cdf(self.size, options["client_zipf_alpha"])
        self.region_cdf = _cdf(self.size, options["region_zipf_alpha"])
        self._heads = {}
        self._region_maps = []
        for region in range(self.regions):
            rng = random.Random(self.seed + 0x6A09E667 + region * 0x9E3779B1)
            a = _coprime_multiplier(rng.randrange(1, self.size + 1), self.size)
            self._region_maps.append((a, rng.randrange(self.size)))

    def sample_rank(self, rng):
        return min(bisect_right(self.client_cdf, rng.random() * self.client_cdf[-1]),
                   self.size - 1)

    def _head(self, client):
        cached = self._heads.get(client)
        if cached is not None:
            return cached
        rng = random.Random(self.seed + 0xBB67AE85 + client * 0x85EBCA77)
        chosen, seen = [], set()
        attempts = 0
        while len(chosen) < self.head_size and attempts < max(100, 100 * self.head_size):
            attempts += 1
            rank = min(bisect_right(self.region_cdf, rng.random() * self.region_cdf[-1]),
                       self.size - 1)
            if rank not in seen:
                seen.add(rank)
                chosen.append(rank)
        # Extremely large alphas can make all but a few CDF buckets smaller
        # than floating-point resolution.  Complete the bijection rather than
        # allowing rejection sampling to stall.
        if len(chosen) < self.head_size:
            for rank in range(self.size):
                if rank not in seen:
                    chosen.append(rank)
                    if len(chosen) == self.head_size:
                        break
        tail_size = self.size - self.head_size
        if tail_size:
            tail_rng = random.Random(self.seed + 0x3C6EF372 + client * 0xC2B2AE3D)
            tail_a = _coprime_multiplier(tail_rng.randrange(1, tail_size + 1), tail_size)
            tail_b = tail_rng.randrange(tail_size)
        else:
            tail_a = tail_b = 0
        cached = (array("I", chosen), array("I", sorted(chosen)), tail_a, tail_b)
        self._heads[client] = cached
        return cached

    def prepare(self, clients):
        """Build rankings before the real-time clock starts."""
        for client in range(clients):
            self._head(client)

    @staticmethod
    def _unrank_excluding(index, excluded):
        """Return the index-th non-excluded non-negative integer."""
        value = index
        while True:
            skipped = bisect_right(excluded, value)
            candidate = index + skipped
            if candidate == value:
                return value
            value = candidate

    def domain_for_rank(self, client, rank):
        head, excluded, tail_a, tail_b = self._head(client)
        if rank < self.head_size:
            regional_rank = head[rank]
        else:
            tail_size = self.size - self.head_size
            if not tail_size:
                regional_rank = head[rank]
            else:
                tail_rank = (tail_a * (rank - self.head_size) + tail_b) % tail_size
                regional_rank = self._unrank_excluding(tail_rank, excluded)
        a, b = self._region_maps[client % self.regions]
        return (a * regional_rank + b) % self.size if self.size > 1 else 0

    def sample(self, client, rng):
        return self.domain_for_rank(client, self.sample_rank(rng))

