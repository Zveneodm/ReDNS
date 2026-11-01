"""Verify saved latency totals against foreground link counts only."""

import argparse
import json
from math import fsum, isclose
from pathlib import Path


def check(directory):
    manifest = json.loads((directory / "comparison.json").read_text())
    rows = {}
    for model, relative in manifest["results"].items():
        result = json.loads((directory / relative).read_text())
        cfg, forwarder = result["config"], result["forwarder"]
        ledger = {
            "client_forwarder_ms": result["client"]["upstream_queries"] * cfg["client_forwarder_RTT"],
            "direct_client_probes_ms": forwarder["peer_queries"] * cfg["client_forwarder_RTT"],
            "cluster_probes_ms": forwarder["cluster_queries"] * cfg["forwarder_forwarder_RTT"],
            "cluster_client_probes_ms": forwarder["cluster_child_queries"] * cfg["client_forwarder_RTT"],
            "manager_lookups_ms": forwarder["manager_queries"] * cfg["forwarder_manager_RTT"],
            "foreground_authority_ms": forwarder["upstream_queries"] * cfg["forwarder_authoritative_RTT"],
            "coalesced_wait_ms": forwarder.get("coalesced_wait_latency_ms", 0),
        }
        total, count = fsum(ledger.values()), result["total_queries"]
        assert len(result["request_latencies_ms"]) == count
        for recorded in (fsum(result["request_latencies_ms"]), result["total_latency_ms"]):
            assert isclose(total, recorded, rel_tol=1e-12, abs_tol=1e-6), (model, total, recorded)
        average = total / count if count else 0.0
        assert isclose(average, result["average_latency_ms"], rel_tol=1e-12, abs_tol=1e-12)
        rows[model] = dict(
            total_queries=count, foreground_rtt_ledger_ms=ledger, ledger_total_ms=total,
            recorded_total_ms=result["total_latency_ms"], recorded_average_ms=result["average_latency_ms"],
            prefetch_queries=result["prefetch_authoritative_queries"],
            excluded_prefetch_rtt_ms=result["prefetch_authoritative_queries"] * cfg["forwarder_authoritative_RTT"],
            checks_passed=True,
        )
    return dict(experimental_source_sha256=manifest["source_fingerprint_sha256"],
                latency_definition="Only foreground request link RTTs; prefetch work and IPC wall time excluded.",
                models=rows)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=root / "res/prefetch")
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("latency_check.json"))
    args = parser.parse_args()
    report = check(args.directory)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    for model, row in report["models"].items():
        print(f"{model}: foreground RTT check passed; average={row['recorded_average_ms']:.9f} ms")
