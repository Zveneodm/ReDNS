"""One demand-funded prefetch budget per cluster, independent of domain shards."""

from dataclasses import dataclass
from fractions import Fraction
from heapq import heapify, heappop, heappush


@dataclass(frozen=True)
class PrefetchCandidate:
    domain: int
    shard: int
    deadline: float
    valid_until: float
    observed_expiry: float
    mean_interval: float
    version: int


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

    def offer(self, shard, advice, now):
        """Replace this shard's previous advice; other shards remain independent."""
        for domain, candidate in list(self.pending.items()):
            if candidate.shard == shard:
                del self.pending[domain]
        for domain, deadline, valid_until, expiry, interval in advice:
            if domain in self.inflight or valid_until <= now:
                continue
            self.version += 1
            candidate = PrefetchCandidate(domain, shard, deadline, valid_until, expiry,
                                          interval, self.version)
            self.pending[domain] = candidate
            heappush(self.heap, (deadline, interval, domain, candidate.version))
        if len(self.heap) > 2 * len(self.pending) + 256:
            self.heap = [(row.deadline, row.mean_interval, row.domain, row.version)
                         for row in self.pending.values()]
            heapify(self.heap)

    def take(self, now, measure):
        if self.available(measure) <= 0:
            return None
        targets = [member for member in self.members if self.active[member] < self.max_inflight]
        if not targets:
            return None
        while self.heap:
            _, _, domain, version = heappop(self.heap)
            candidate = self.pending.get(domain)
            if candidate is None or candidate.version != version:
                continue
            del self.pending[domain]
            if candidate.valid_until <= now:
                continue
            target = min(targets, key=lambda member: (
                self.loads.get(member, float("inf")) + self.active[member],
                (member - domain) % self.num_forwarders))
            self.active[target] += 1
            self.reserved[measure] += 1
            self.inflight[domain] = (target, measure, now)
            return candidate, target
        return None

    def finish(self, domain, success):
        target, measure, issued_at = self.inflight.pop(domain)
        self.active[target] -= 1
        if not success:
            self.refunded[measure] += 1
        return measure, issued_at

    def clear_pending(self):
        self.pending.clear()
        self.heap.clear()
