"""Verify the published pair against its standard-config snapshot and RTT ledger."""

import argparse
from hashlib import sha256
import json
from math import fsum, isclose
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from lib.comparison import PREFETCH_ABLATION_MODELS, validate_pair
from lib.config import load_config, validate_config
from lib.metrics import validate_results


def verify(directory, config_path):
    manifest = json.loads((directory / "comparison.json").read_text())
    config = load_config(config_path)
    if set(manifest["results"]) != set(PREFETCH_ABLATION_MODELS):
        raise RuntimeError("expected only baseline-prefetch and ReDNS-prefetch")
    if validate_config(manifest["common_config"]) != config:
        raise RuntimeError("published experiment differs from the standard-config snapshot")
    validate_pair(manifest["models"], manifest["common_config"], manifest["cpu_pool"])
    certificate = dict(standard_config_matches=True, checks_passed=True,
                       source_fingerprint_sha256=manifest["source_fingerprint_sha256"], models={})
    for model, relative in manifest["results"].items():
        path = directory / relative
        result = json.loads(path.read_text())
        validate_results(result)
        if validate_config(result["config"]) != config or not result["timing_valid"]:
            raise RuntimeError(f"invalid published config/timing for {model}")
        if not result["prefetch_enabled"] or result["cooperation_enabled"] != (model == "ReDNS-prefetch"):
            raise RuntimeError(f"incorrect feature switches for {model}")
        summary = manifest["models"][model]
        for field in ("workload_fingerprint_sha256", "ttl_fingerprint_sha256", "total_queries",
                      "warm_up_queries", "authoritative_queries", "prefetch_authoritative_queries"):
            if result[field] != summary[field]:
                raise RuntimeError(f"manifest/result mismatch: {model}.{field}")
        forwarder = result["forwarder"]
        if model == "baseline-prefetch":
            for field in ("peer_queries", "cluster_queries", "cluster_queries_received",
                          "manager_queries", "manager_updates_sent", "records_transferred",
                          "directory_stores_received", "directory_removes_received"):
                if forwarder[field]:
                    raise RuntimeError(f"baseline-prefetch enabled a ReDNS mechanism: {field}")
            if result["manager"]["directory_entries"] or result["manager"]["tracked_versions"]:
                raise RuntimeError("baseline-prefetch unexpectedly maintains region metadata")
        ledger = dict(
            client_forwarder_ms=result["client"]["upstream_queries"] * config["client_forwarder_RTT"],
            direct_client_probes_ms=forwarder["peer_queries"] * config["client_forwarder_RTT"],
            cluster_probes_ms=forwarder["cluster_queries"] * config["forwarder_forwarder_RTT"],
            cluster_client_probes_ms=forwarder["cluster_child_queries"] * config["client_forwarder_RTT"],
            manager_lookups_ms=forwarder["manager_queries"] * config["forwarder_manager_RTT"],
            foreground_authority_ms=forwarder["upstream_queries"] * config["forwarder_authoritative_RTT"],
            coalesced_wait_ms=forwarder["coalesced_wait_latency_ms"],
        )
        total, count = fsum(ledger.values()), result["total_queries"]
        if len(result["request_latencies_ms"]) != count:
            raise RuntimeError(f"incomplete latency trace for {model}")
        for actual in (fsum(result["request_latencies_ms"]), result["total_latency_ms"]):
            if not isclose(total, actual, rel_tol=1e-12, abs_tol=1e-6):
                raise RuntimeError(f"foreground RTT ledger does not match for {model}")
        if not isclose(total / count if count else 0, result["average_latency_ms"],
                       rel_tol=1e-12, abs_tol=1e-12):
            raise RuntimeError(f"incorrect mean latency for {model}")
        digest = sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        certificate["models"][model] = dict(
            result_file=relative, result_sha256=digest.hexdigest(),
            total_queries=count, authority_trace_count=sum(result["request_authority_queries"]),
            authoritative_queries=result["authoritative_queries"],
            prefetch_authoritative_queries=result["prefetch_authoritative_queries"],
            foreground_rtt_ledger_ms=ledger, ledger_total_ms=total,
            average_latency_ms=result["average_latency_ms"],
            excluded_prefetch_rtt_ms=(result["prefetch_authoritative_queries"] *
                                      config["forwarder_authoritative_RTT"]),
            checks_passed=True,
        )
        print(f"{model}: {count:,} requests; config, flow, timing, source flags and RTT ledger verified")
        del result
    return certificate


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT / "res/prefetch-ablation")
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("verification.json"))
    args = parser.parse_args()
    report = verify(args.directory, args.config)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
