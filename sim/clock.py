"""One shared clock for all cache deadlines in a simulation."""

from time import perf_counter_ns


class SimulationClock:
    def __init__(self):
        self.now = 0

    def advance(self, seconds=1):
        if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 0:
            raise ValueError("time must be a non-negative integer")
        self.now += seconds


class RealtimeClock:
    """All processes use the same monotonic epoch and wall/simulation ratio."""

    def __init__(self):
        self.start_ns = None
        self.scale = None
        self.stop_ns = None

    def start(self, start_ns, scale):
        self.start_ns = start_ns
        self.scale = scale
        self.stop_ns = None

    @property
    def now(self):
        if self.start_ns is None:
            return 0.0
        timestamp = self.stop_ns if self.stop_ns is not None else perf_counter_ns()
        return max(0.0, (timestamp - self.start_ns) / (self.scale * 1_000_000_000))

    def freeze(self, stop_ns):
        self.stop_ns = stop_ns
