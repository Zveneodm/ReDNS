"""Sum foreground DNS round trips, excluding background cache maintenance."""

from contextlib import contextmanager


class LatencyTracker:
    def __init__(self):
        self.latency_ms = 0
        self._background_depth = 0

    def reset(self):
        self.latency_ms = 0

    def add_rtt(self, milliseconds):
        if self._background_depth == 0:
            self.latency_ms += milliseconds

    @property
    def is_background(self):
        return self._background_depth > 0

    @contextmanager
    def background(self):
        """Exclude nested transfers and refresh lookups from request latency."""
        self._background_depth += 1
        try:
            yield
        finally:
            self._background_depth -= 1
