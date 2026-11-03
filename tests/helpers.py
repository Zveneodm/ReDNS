"""Small, fixed test workloads and an in-memory IPC transport."""

from lib.clock import ManualClock
from lib.config import validate_config
from lib.realtime import simulation_roles, _Layer


def config(**overrides):
    return validate_config({
        "num_clients": 4, "num_forwarders": 2, "num_domains": 20,
        "client_cache_size": 2, "forwarder_cache_size": 2,
        "warm_up_seconds": 0, "duration_seconds": 2, "queries_per_second": 100,
        "time_scale": 0.02, "max_in_flight": 32, "calibration_requests": 30,
        # Most protocol tests intentionally compress seconds into milliseconds.
        # Timing rejection has separate tests using production limits.
        "timing": {"max_mean_lag_seconds": 10, "max_window_lag_seconds": 20,
                   "max_overrun_seconds": 20},
        "ttl_range": [100, 100], **overrides,
    })


class ProtocolHarness:
    """Run the real message handlers with an explicitly controlled clock."""

    def __init__(self, model="ReDNS", **overrides):
        self.config = config(**overrides)
        self.clock = ManualClock()
        self.layers = {role: _Layer(role, model, self.config, {}, None, None)
                       for role in simulation_roles(self.config, model)}
        for layer in self.layers.values():
            layer.clock = self.clock
            for node in layer.nodes:
                node.clock = self.clock
            if layer.authority is not None:
                layer.authority.clock = self.clock
            for manager in layer.managers.values():
                manager.clock = self.clock

    def drain(self):
        for _ in range(10000):
            worked = False
            for layer in self.layers.values():
                while layer.outbox:
                    role, message = layer.outbox.popleft()
                    self.layers[role]._message(message)
                    worked = True
            if not worked:
                return
        raise AssertionError("protocol did not drain")

    def query(self, client, domain, measure=True):
        replies = []
        self.layers["client"]._query(client, domain, True, measure, replies.append)
        self.drain()
        if len(replies) != 1:
            raise AssertionError("query did not produce exactly one reply")
        return replies[0]
