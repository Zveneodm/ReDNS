"""Direct-child cache directory populated only by actual cache notifications."""

from collections import Counter


class CopyDirectory:
    """Domain -> {direct child ID: absolute expiry}; never stores DNS answers.

    Notifications are applied in sender order. Expiry-aware removal also keeps
    a delayed failed-probe response from deleting a newer, longer-lived copy.
    Counts are occupancy hints: expired entries are pruned lazily by domain.
    """
    def __init__(self):
        self.by_name = {}
        self.child_counts = Counter()
        self.entries = 0

    def store(self, child, name, expiry):
        row = self.by_name.setdefault(name, {})
        if child not in row:
            self.child_counts[child] += 1
            self.entries += 1
        row[child] = expiry

    def remove(self, child, name, removed_expiry=float("inf")):
        row = self.by_name.get(name)
        if row is None or child not in row or row[child] > removed_expiry + 1e-9:
            return
        del row[child]
        self.entries -= 1
        self.child_counts[child] -= 1
        if not self.child_counts[child]:
            del self.child_counts[child]
        if not row:
            del self.by_name[name]

    def choose(self, name, now, *, exclude=()):
        row = self.by_name.get(name)
        if not row:
            return None
        for child in [child for child, expiry in row.items() if expiry <= now]:
            self.remove(child, name, now)
        row = self.by_name.get(name)
        return min((child for child in row if child not in exclude), default=None) if row else None

    def deadline(self, child, name):
        return self.by_name.get(name, {}).get(child, 0)

    def latest_deadline(self, name, now):
        """Latest known live copy, without reading any child's cache."""
        self.choose(name, now)
        return max(self.by_name.get(name, {}).values(), default=0)


def spare_target(targets, occupancy, capacity, hash_value):
    """Stable placement in spare slots; the receiver rechecks its capacity."""
    if not targets or not capacity:
        return None
    start = hash_value % len(targets)
    for offset in range(len(targets)):
        child = targets[(start + offset) % len(targets)]
        if occupancy.get(child, 0) < capacity:
            return child
    return None
