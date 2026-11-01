"""One merged availability entry per domain and forwarder-managed region."""

from collections import Counter


MANAGER_FIELDS = (
    "queries", "directory_hits", "directory_misses",
    "updates_received", "updates_applied", "updates_ignored",
    "feedback_received", "feedback_applied", "feedback_ignored",
)


def new_manager_stats():
    return Counter(dict.fromkeys(MANAGER_FIELDS, 0))


def empty_manager_metrics():
    return {**dict.fromkeys(MANAGER_FIELDS, 0), "directory_entries": 0, "tracked_versions": 0}


class ClusterManager:
    """A directory, with no DNS answers or knowledge of individual clients.

    An advertised deadline is the latest expiry among the forwarder's own
    cache and its known direct-client copies. Versions are monotonic per
    publishing forwarder. Tombstones keep delayed updates from resurrecting a
    withdrawn entry; feedback only invalidates the exact version queried.
    """

    def __init__(self, cluster_id, members, clock):
        self.cluster_id = cluster_id
        self.members = frozenset(members)
        self.clock = clock
        self.by_domain = {}
        self.versions = {}
        self.entries = 0
        self.stats = new_manager_stats()

    def _check_member(self, forwarder):
        if forwarder not in self.members:
            raise ValueError("forwarder is outside this manager's cluster")

    def _remove(self, domain, forwarder):
        row = self.by_domain.get(domain)
        if row is None or forwarder not in row:
            return
        del row[forwarder]
        self.entries -= 1
        if not row:
            del self.by_domain[domain]

    def update(self, forwarder, domain, deadline, version):
        self._check_member(forwarder)
        self.stats["updates_received"] += 1
        key = (forwarder, domain)
        if version <= self.versions.get(key, 0):
            self.stats["updates_ignored"] += 1
            return
        self.versions[key] = version
        self.stats["updates_applied"] += 1
        if deadline <= self.clock.now:
            self._remove(domain, forwarder)
            return
        row = self.by_domain.setdefault(domain, {})
        if forwarder not in row:
            self.entries += 1
        row[forwarder] = (deadline, version)

    def lookup(self, requester, domain):
        self._check_member(requester)
        self.stats["queries"] += 1
        row = self.by_domain.get(domain, {})
        for forwarder, (deadline, _) in list(row.items()):
            if deadline <= self.clock.now:
                self._remove(domain, forwarder)
        candidates = [(forwarder, deadline, version)
                      for forwarder, (deadline, version) in sorted(self.by_domain.get(domain, {}).items())
                      if forwarder != requester]
        self.stats["directory_hits" if candidates else "directory_misses"] += 1
        return candidates

    def invalidate(self, reporter, forwarder, domain, version):
        self._check_member(reporter)
        self._check_member(forwarder)
        if reporter == forwarder:
            raise ValueError("manager feedback must refer to a neighboring region")
        self.stats["feedback_received"] += 1
        row = self.by_domain.get(domain, {})
        entry = row.get(forwarder)
        if entry is None or entry[1] != version:
            self.stats["feedback_ignored"] += 1
            return
        self._remove(domain, forwarder)
        self.stats["feedback_applied"] += 1

    def metrics(self):
        return {**self.stats, "directory_entries": self.entries, "tracked_versions": len(self.versions)}
