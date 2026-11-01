"""One demand-funded prefetch budget per cluster, independent of domain shards."""

from dataclasses import dataclass
from fractions import Fraction
from heapq import heapify, heappop, heappush
from typing import Optional


@dataclass(frozen=True)
class PrefetchCandidate:
    domain: int
    shard: int
    deadline: float
    valid_until: float
    observed_expiry: float
    mean_interval: float
    version: int
    target: Optional[int] = None

    @property
    def key(self):
        return self.domain if self.target is None else (self.target, self.domain)


class ClusterPrefetchBudget:
    """Reserve one credit per task; failed/skipped tasks return their credit.

    Warm-up and formal demand have separate ledgers. Decimal ratios use exact
    fractions: fifteen percent of twenty requests always grants three credits.
    A credit can be spent only once across all manager domain shards.
    """

    def __init__(self, ratio, members, max_inflight, num_forwarders):
        self.ratio = Fraction(str(ratio))
        self.members = tuple(sorted(members))
        self.max_inflight = max_inflight
        self.num_forwarders = num_forwarders
        self.demand = [0, 0]
        self.reserved = [0, 0]
        self.refunded = [0, 0]
        self.loads = {}
        self.active = dict.fromkeys(self.members, 0)
        self.pending = {}
        self.heap = []
        self.inflight = {}
        self.version = 0

    def allowance(self, measure):
        return self.demand[measure] * self.ratio.numerator // self.ratio.denominator

    def available(self, measure):
        return self.allowance(measure) - self.reserved[measure] + self.refunded[measure]

    def fund(self, counts):
        changes = []
        for measure, count in enumerate(counts):
            previous = self.allowance(measure)
            self.demand[measure] += count
            changes.append(self.allowance(measure) - previous)
        return changes

    def offer(self, shard, advice, now, *, target=None):
        """Replace this shard's previous advice; other shards remain independent."""
        for domain, candidate in list(self.pending.items()):
            if candidate.shard == shard and candidate.target == target:
                del self.pending[domain]
        for domain, deadline, valid_until, expiry, interval in advice:
            key = domain if target is None else (target, domain)
            if target is not None and target not in self.members:
                raise ValueError("prefetch target is outside its budget cluster")
            if key in self.inflight or valid_until <= now:
                continue
            self.version += 1
            candidate = PrefetchCandidate(domain, shard, deadline, valid_until, expiry,
                                          interval, self.version, target)
            self.pending[key] = candidate
            heappush(self.heap, (deadline, interval, domain, candidate.version, key))
        if len(self.heap) > 2 * len(self.pending) + 256:
            self.heap = [(row.deadline, row.mean_interval, row.domain, row.version, row.key)
                         for row in self.pending.values()]
            heapify(self.heap)

    def take(self, now, measure):
        if self.available(measure) <= 0:
            return None
        targets = [member for member in self.members if self.active[member] < self.max_inflight]
        if not targets:
            return None
        deferred = []
        try:
            while self.heap:
                item = heappop(self.heap)
                _, _, domain, version, key = item
                candidate = self.pending.get(key)
                if candidate is None or candidate.version != version:
                    continue
                if candidate.valid_until <= now:
                    del self.pending[key]
                    continue
                if candidate.target is not None and candidate.target not in targets:
                    # Local advice must wait for its own forwarder; another
                    # idle member cannot consume a record it cannot share.
                    deferred.append(item)
                    continue
                del self.pending[key]
                target = (candidate.target if candidate.target is not None else
                          min(targets, key=lambda member: (
                              self.loads.get(member, float("inf")) + self.active[member],
                              (member - domain) % self.num_forwarders)))
                self.active[target] += 1
                self.reserved[measure] += 1
                self.inflight[key] = (target, measure, now)
                return candidate, target
        finally:
            for item in deferred:
                heappush(self.heap, item)
        return None

    def finish(self, key, success):
        target, measure, issued_at = self.inflight.pop(key)
        self.active[target] -= 1
        if not success:
            self.refunded[measure] += 1
        return measure, issued_at

    def clear_pending(self):
        self.pending.clear()
        self.heap.clear()
