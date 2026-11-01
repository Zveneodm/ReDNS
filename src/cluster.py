"""Static, disjoint forwarder groups used only for bounded cache lookups."""


class ForwarderClusters:
    """Consecutive IDs form a group; the final group may be smaller."""

    def __init__(self, count, size):
        for name, value in (("count", count), ("size", size)):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"cluster {name} must be a positive integer")
        self.count = count
        self.size = size

    def _validate_id(self, forwarder_id):
        if (isinstance(forwarder_id, bool) or not isinstance(forwarder_id, int)
                or not 0 <= forwarder_id < self.count):
            raise ValueError("forwarder ID is outside the topology")

    def members(self, forwarder_id):
        self._validate_id(forwarder_id)
        start = forwarder_id // self.size * self.size
        return range(start, min(start + self.size, self.count))

    def neighbors(self, forwarder_id):
        return tuple(member for member in self.members(forwarder_id) if member != forwarder_id)

    def can_query(self, source, target):
        self._validate_id(source)
        self._validate_id(target)
        return source != target and source // self.size == target // self.size
