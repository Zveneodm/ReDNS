"""Export deterministic protocol snapshots for the directory migration audit."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.helpers import ProtocolHarness


def capture():
    cases = {}
    for model in ("baseline", "baseline-prefetch", "ReDNS", "ReDNS-prefetch"):
        for policy in ("lru", "random"):
            harness = ProtocolHarness(
                model, num_clients=12, num_forwarders=4, forwarder_cluster_size=2,
                client_cache_size=3, forwarder_cache_size=3, num_domains=20,
                ttl_range=[2, 3], replacement_policy=policy, warm_up_seconds=2, duration_seconds=25,
                prefetch={"report_seconds": 1, "advice_seconds": 1, "budget_ratio": 0.5},
            )
            answers = []
            for step in range(180):
                now = step * 0.13
                harness.clock.advance_to(now)
                for client in (step % 12, (step + 4) % 12, (step + 8) % 12):
                    harness.layers["client"]._query(
                        client, (step * 7 + step // 12) % 20, True, now >= 2, answers.append)
                harness.drain()
                for layer in harness.layers.values():
                    layer.prefetch_tick()
                harness.drain()
            harness.clock.advance_to(27)
            for layer in harness.layers.values():
                layer.prefetch_tick()
            harness.drain()
            cases[f"{model}/{policy}"] = dict(
                answers=answers,
                nodes={role: [layer._select(i, True).metrics() for i in layer.node_ids]
                       for role, layer in harness.layers.items() if role in ("client", "forwarder")},
                managers={str(cluster): layer._select_manager(cluster, True).metrics()
                          for layer in harness.layers.values() for cluster in layer.managers},
                authority=harness.layers["authority"].authority.queries,
                prefetch_authority=harness.layers["authority"].authority.prefetch_queries,
            )
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    cases = capture()
    args.output.write_text(json.dumps(cases, sort_keys=True, separators=(",", ":")))
    print(f"deterministic snapshots: {len(cases)}; sha256: {sha256(args.output.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    main()
