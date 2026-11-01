"""Deterministic examples of policy/framework limits, separate from benchmarks."""

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from sim.config import validate_config
from src.prefetch import Forestall
from tests.helpers import ProtocolHarness


def main():
    tracker = Forestall(validate_config({})["prefetch"])
    for when in (0, 4, 8):
        tracker.observe(0, when, when + 2, 2)
    for when in (10.7, 11.4, 11.5):
        tracker.observe(100, when, when + 2, 2)
    advice = tracker.advise(12)
    collapsed = dict(
        advice=advice,
        average_interarrivals={str(domain): entry.intervals.mean
                               for domain, entry in tracker.tracking.items()},
        explanation="Both deadlines collapse to now; numeric domain ID precedes request frequency.")

    harness = ProtocolHarness("ReDNS-prefetch", num_forwarders=2, forwarder_cluster_size=2,
                              client_cache_size=0, forwarder_cache_size=1,
                              duration_seconds=10, ttl_range=[100, 100],
                              prefetch={"report_seconds": 1, "advice_seconds": 1})
    for when in (0, 0.5, 0.9):
        harness.clock.advance_to(when)
        harness.query(0, 7)
    harness.clock.advance_to(0.95)
    harness.query(0, 8)
    harness.clock.advance_to(1)
    forwarder = harness.layers["forwarder"]
    forwarder.prefetch_tick()
    harness.drain()
    manager = harness.layers["manager"]
    tracker = manager.prefetchers[0]
    absent = dict(
        actual_cache_has_record=7 in forwarder.nodes[0].cache,
        manager_has_live_region=bool(manager.managers[0].by_domain.get(7)),
        observed_expires_at=tracker.tracking[7].expires_at,
        advised=any(domain == 7 for _, domain in tracker.advise(1)),
    )
    previous = harness.layers["authority"].authority.queries
    harness.clock.advance_to(1.1)
    harness.query(0, 7)
    absent["next_demand_causes_origin"] = harness.layers["authority"].authority.queries > previous

    harness = ProtocolHarness("ReDNS-prefetch", forwarder_cluster_size=2,
                              client_cache_size=0, forwarder_cache_size=0)
    reply = harness.query(0, 7)
    framework = dict(configured_authority_rtt_ms=harness.config["forwarder_authoritative_RTT"],
                     returned_latency_ms=reply[2], clock_after_query=harness.clock.now,
                     forwarder_active_after_query=dict(harness.layers["forwarder"].prefetch_active))
    report = dict(collapsed_priority=collapsed, ttl_is_not_cache_presence=absent,
                  accumulated_rtt_is_not_service_occupancy=framework)
    Path(__file__).with_name("audit_probes.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
