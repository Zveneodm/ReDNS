"""Detect arrival backlog without changing the simulated clock or dropping work."""


class SimulationOverload(RuntimeError):
    """A discarded trial could not keep pace with its planned arrival schedule."""

    def __init__(self, report):
        self.report = report
        super().__init__("simulation cannot sustain the offered load: " +
                         "; ".join(report["reasons"]))


class TimingMonitor:
    """Track lag in planned, one-simulation-second windows, including warm-up."""

    def __init__(self, limits):
        self.limits = limits
        self.windows = []
        self.window = None
        self.count = 0
        self.total = self.maximum = 0.0
        self.streak = self.longest_streak = 0
        self.sustained = False
        self.all_count = 0
        self.all_total = self.all_maximum = 0.0

    def _close(self):
        if self.window is None:
            return
        mean = self.total / self.count
        self.streak = self.streak + 1 if mean > self.limits["max_window_lag_seconds"] else 0
        self.longest_streak = max(self.longest_streak, self.streak)
        self.sustained |= self.streak >= self.limits["consecutive_lag_windows"]
        self.windows.append(dict(start_seconds=self.window, queries=self.count,
                                 mean_lag_seconds=mean, max_lag_seconds=self.maximum))
        self.window = None
        self.count = 0
        self.total = self.maximum = 0.0

    def observe(self, scheduled, lag):
        window = int(scheduled)
        if self.window != window:
            previous = self.window
            self._close()
            if previous is not None and window > previous + 1:
                self.streak = 0
            self.window = window
        lag = max(0.0, lag)
        self.count += 1
        self.total += lag
        self.maximum = max(self.maximum, lag)
        self.all_count += 1
        self.all_total += lag
        self.all_maximum = max(self.all_maximum, lag)

    def report(self, *, overrun=0.0, finish=False):
        if finish:
            self._close()
        mean = self.all_total / self.all_count if self.all_count else 0.0
        reasons = []
        if self.sustained:
            reasons.append(f"lag exceeded {self.limits['max_window_lag_seconds']:g}s for "
                           f"{self.longest_streak} consecutive arrival windows")
        if mean > self.limits["max_mean_lag_seconds"]:
            reasons.append(f"mean arrival lag {mean:.6f}s exceeds limit")
        if overrun > self.limits["max_overrun_seconds"]:
            reasons.append(f"run overran the arrival horizon by {overrun:.6f}s")
        return dict(valid=not reasons, reasons=reasons, limits=dict(self.limits),
                    mean_lag_seconds=mean, max_lag_seconds=self.all_maximum,
                    overrun_seconds=max(0.0, overrun), longest_lag_streak=self.longest_streak,
                    scheduled_queries=self.all_count, windows=list(self.windows))
