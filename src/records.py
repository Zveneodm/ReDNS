"""Immutable DNS answers shared by the baseline and ReDNS models."""

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class Record:
    """A synthetic domain ID, an absolute deadline and the original TTL.

    Copying an answer between caches preserves ``expires_at``. A cache hit
    never renews the lifetime; only a new authority lookup starts a fresh TTL.
    """

    domain: int
    expires_at: float
    auth_ttl: float

    def __post_init__(self):
        if isinstance(self.domain, bool) or not isinstance(self.domain, int) or self.domain < 0:
            raise ValueError("domain must be a non-negative integer ID")
        for name in ("expires_at", "auth_ttl"):
            value = getattr(self, name)
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not isfinite(value) or value < 0):
                raise ValueError(f"{name} must be finite and non-negative")

    def remaining_ttl(self, now):
        return max(0.0, self.expires_at - now)

    def is_expired(self, now):
        return self.expires_at <= now
