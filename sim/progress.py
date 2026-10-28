"""Compact single-line experiment progress, including the warm-up window."""

import sys


class Progress:
    def __init__(self, total_seconds, stream=None):
        self.total = total_seconds
        self.stream = sys.stdout if stream is None else stream
        self.last = None
        self.closed = False

    def update(self, seconds, *, finished=False):
        limit = self.total if finished else max(0, self.total - 1)
        current = min(limit, max(0, int(seconds)))
        if current != self.last:
            self.stream.write(f"\r{current}s/{self.total}s")
            self.stream.flush()
            self.last = current

    def finish(self):
        self.update(self.total, finished=True)
        self.close()

    def close(self):
        if not self.closed and self.last is not None:
            self.stream.write("\n")
            self.stream.flush()
        self.closed = True
