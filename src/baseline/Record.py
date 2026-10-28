"""A DNS answer with a remaining TTL and a constant authoritative TTL."""

from math import isfinite


class Record:
    def __init__(self, r_name, TTL, auth_TTL=None, *, clock=None):
        if not isinstance(r_name, str) or not r_name:
            raise ValueError("r_name must be a non-empty string")
        self.r_name = r_name
        self._clock = clock
        self._expiry_ref = None
        self.ttl = TTL
        self.auth_ttl = TTL if auth_TTL is None else auth_TTL

    @staticmethod
    def _validate_seconds(value):
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not isfinite(value) or value < 0):
            raise ValueError("TTL/time must be finite and non-negative")
        return value

    @property
    def TTL(self):
        if self._clock is None:
            return self._ttl
        remaining = self._expires_at - self._clock.now
        return remaining if remaining > 0 else 0

    @TTL.setter
    def TTL(self, value):
        value = self._validate_seconds(value)
        if self._clock is None:
            self._ttl = value
        else:
            self._expires_at = self._clock.now + value
            if self._expiry_ref is not None:
                index = self._expiry_ref()
                if index is not None:
                    index.update(self)

    # Preserve both field names; clock-free records still support countdown().
    @property
    def ttl(self):
        return self.TTL

    @ttl.setter
    def ttl(self, value):
        self.TTL = value

    @property
    def auth_ttl(self):
        return self.auth_TTL

    @auth_ttl.setter
    def auth_ttl(self, value):
        self.auth_TTL = self._validate_seconds(value)

    def countdown(self, seconds=1):
        self._validate_seconds(seconds)
        self.TTL = max(0, self.TTL - seconds)

    def is_expired(self):
        if self._clock is None:
            return self._ttl <= 0
        return self._expires_at <= self._clock.now

    def copy(self):
        """Each cache and caller owns its answer and countdown independently."""
        return Record(self.r_name, self.TTL, self.auth_TTL)
