"""Forestall-inspired transaction tracking and windowed advice.

Reference: Ferreira and Oki, TNSM 2026, doi:10.1109/TNSM.2026.3704549,
Section III-B. This is a cluster adaptation, not the authors' implementation.
Only observed demand feeds Welford statistics; prefetches never feed demand.
H expires inactive tracking entries (statistics are reset after inactivity).
"""

from dataclasses import dataclass, field
from math import sqrt


@dataclass
class Moments:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add(self, value):
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        self.m2 += delta * (value - self.mean)


@dataclass
class TrackedDomain:
    last_arrival: float
    expires_at: float
    intervals: Moments = field(default_factory=Moments)
    ttls: Moments = field(default_factory=Moments)
    last_advice: float = -1.0
    failed_advice: int = 0
    advice_pending: bool = False


class Forestall:
    def __init__(self, config):
        self.cfg = config
        self.tracking = {}

    def observe(self, domain, arrival, expiry, ttl):
        entry = self.tracking.get(domain)
        if entry is None or arrival - entry.last_arrival > self.cfg["history_seconds"]:
            entry = self.tracking[domain] = TrackedDomain(arrival, expiry)
        else:
            # A late reply can put an older transaction in a newer report.
            # Do not fabricate a negative or zero interval for that transaction.
            if arrival >= entry.last_arrival:
                entry.intervals.add(arrival - entry.last_arrival)
                entry.last_arrival = arrival
            entry.expires_at = max(entry.expires_at, expiry)
        entry.ttls.add(ttl)

    def refreshed(self, domain, expiry):
        entry = self.tracking.get(domain)
        if entry is not None:
            entry.expires_at = expiry

    def advise(self, now):
        """Return (deadline, domain), earliest deadline first.

        Adaptation: skip entries still valid throughout A, and use
        mean + gamma * stddev as the inactivity tolerance. Deadlines are
        min(predicted arrival, observed expiration), clamped to now.
        """
        cfg = self.cfg
        result = []
        for domain, entry in list(self.tracking.items()):
            if entry.advice_pending:
                entry.failed_advice = (entry.failed_advice + 1
                                       if entry.last_arrival <= entry.last_advice else 0)
                entry.advice_pending = False
            if (entry.last_arrival < now - cfg["history_seconds"] or
                    (cfg["zeta"] is not None and entry.failed_advice > cfg["zeta"])):
                del self.tracking[domain]
                continue
            n = entry.intervals.count
            if n < cfg["alpha"] or n <= cfg["beta"] or entry.ttls.mean <= 0:
                continue
            if cfg["epsilon"] is not None and entry.failed_advice > cfg["epsilon"]:
                continue
            interval = entry.intervals.mean
            deviation = sqrt(max(0.0, entry.intervals.m2 / (n - cfg["beta"])))
            if now - entry.last_arrival > interval + cfg["gamma"] * deviation:
                continue
            expected = entry.last_arrival + interval
            if expected > now + cfg["advice_seconds"] or entry.expires_at > now + cfg["advice_seconds"]:
                continue
            result.append((max(now, min(expected, entry.expires_at)), domain))
        return sorted(result)

    def mark_advised(self, domain, now):
        entry = self.tracking[domain]
        entry.last_advice = now
        entry.advice_pending = True
