"""Read the saved comparison; recover paired outcomes and request histories.

No historical cache contents or prefetch-task traces were saved. This script
therefore reports observed outcomes, not causal cache-miss classifications.
"""

from array import array
from collections import Counter, defaultdict
from hashlib import sha256
import json
from pathlib import Path
from struct import pack
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from sim.analyze_origin import authority_threshold, authority_ttls, workload_arrivals
from sim.metrics import validate_results


def main():
    directory = ROOT / "res/prefetch"
    manifest = json.loads((directory / "comparison.json").read_text())
    models = ("ReDNS", "ReDNS-prefetch")
    latencies, wall, metadata, stats, allocation = {}, {}, {}, {}, {}
    for model in models:
        result = json.loads((directory / manifest["results"][model]).read_text())
        validate_results(result)
        threshold = authority_threshold(result)
        latencies[model] = array("d", result["request_latencies_ms"])
        wall[model] = array("d", result["request_wall_latencies_ms"])
        stats[model] = {key: result[key] for key in ("client", "forwarder", "manager")}
        allocation[model] = {
            field: dict(minimum=min(node[field] for node in result["forwarders"]),
                        maximum=max(node[field] for node in result["forwarders"]))
            for field in ("prefetch_tasks_received", "prefetch_started", "prefetch_skipped")}
        metadata[model] = {key: result[key] for key in (
            "config", "workload_fingerprint_sha256", "ttl_fingerprint_sha256",
            "total_queries", "warm_up_queries", "authoritative_queries", "timing_validation")}
        metadata[model]["authority_threshold"] = threshold
        del result
    cfg = manifest["common_config"]
    ttls = authority_ttls(cfg)
    ttl_hash = sha256(json.dumps(ttls, separators=(",", ":")).encode()).hexdigest()
    assert all(row["ttl_fingerprint_sha256"] == ttl_hash for row in metadata.values())
    assert metadata[models[0]]["config"] == metadata[models[1]]["config"] == cfg
    history = bytearray(cfg["num_domains"])
    domain_demand = Counter()
    by_ttl = defaultdict(Counter)
    transitions = Counter()
    time_buckets = defaultdict(Counter)
    wall_second_phase = defaultdict(Counter)
    origins = {model: Counter() for model in models}
    fingerprint = sha256()
    warm = index = 0
    last_origin = {model: {} for model in models}
    fresh_repeat = {model: Counter() for model in models}
    threshold = metadata[models[0]]["authority_threshold"]
    assert threshold == metadata[models[1]]["authority_threshold"]
    for when, client, domain in workload_arrivals(cfg):
        fingerprint.update(pack("!dQQ", when, client, domain))
        count = history[domain]
        history[domain] = min(255, count + 1)
        if when < cfg["warm_up_seconds"]:
            warm += 1
            continue
        ttl = ttls[domain]
        phase = min(9, int((when % 1) * 10))
        wall_second_phase[phase]["requests"] += 1
        for model in models:
            wall_second_phase[phase][model + "_wall_ms_sum"] += wall[model][index]
            wall_second_phase[phase][model + "_over_5ms"] += wall[model][index] > 5
        domain_demand[domain] += 1
        old, new = (latencies[model][index] for model in models)
        origins_now = [old >= threshold, new >= threshold]
        bucket = int((when - cfg["warm_up_seconds"]) // 60)
        row = by_ttl[ttl]
        row["requests"] += 1
        transitions[f"{old:g}->{new:g}"] += 1
        time_buckets[bucket]["requests"] += 1
        time_buckets[bucket]["latency_saved_ms"] += old - new
        row["latency_saved_ms"] += old - new
        if old > new:
            row["faster_requests"] += 1
        elif old < new:
            row["slower_requests"] += 1
        if origins_now == [True, False]:
            row["origin_to_cache"] += 1
        elif origins_now == [False, True]:
            row["cache_to_origin"] += 1
        for model, is_origin in zip(models, origins_now):
            if not is_origin:
                continue
            row[model + "_origins"] += 1
            row[model + ("_first_global_origins" if count == 0 else "_repeat_origins")] += 1
            time_buckets[bucket][model + "_origins"] += 1
            origins[model]["first_global" if count == 0 else "repeat_global"] += 1
            origins[model]["previous_requests_" + (str(count) if count < 2 else "2plus")] += 1
            previous = last_origin[model].get(domain)
            # Conservative certificate: a previous formal origin request had
            # completed before this request was even scheduled, and its answer
            # could not yet have expired at this request's completion. The
            # global lag maximum bounds every issue time, including warm-up.
            # This rules out TTL alone, but cannot distinguish capacity loss
            # from missing directory knowledge without historical snapshots.
            max_lag = metadata[model]["timing_validation"]["max_lag_seconds"]
            finish_bound = when + max_lag + wall[model][index] / (1000 * cfg["time_scale"])
            if previous is not None:
                prev_when, prev_finish_bound = previous
                fresh_repeat[model]["origins_with_prior_formal_origin"] += 1
                if when > prev_finish_bound + ttl:
                    fresh_repeat[model]["previous_formal_answer_definitely_expired"] += 1
                if prev_finish_bound < when and prev_when + ttl > finish_bound:
                    fresh_repeat[model]["certified_non_ttl_repeat_origins"] += 1
                    fresh_repeat[model][f"ttl_{ttl}"] += 1
            last_origin[model][domain] = (when, finish_bound)
        index += 1
        if index % 2_000_000 == 0:
            print(f"Replayed {index:,} formal requests", flush=True)
    assert all(fingerprint.hexdigest() == row["workload_fingerprint_sha256"] for row in metadata.values())
    assert all(index == row["total_queries"] and warm == row["warm_up_queries"] for row in metadata.values())
    assert all(sum(origins[model][field] for field in ("first_global", "repeat_global")) ==
               metadata[model]["authoritative_queries"] for model in models)
    plain, prefetch = (stats[model] for model in models)
    manager, fw = prefetch["manager"], prefetch["forwarder"]
    report = dict(
        source_comparison=str(directory / "comparison.json"),
        experimental_source_sha256=manifest["source_fingerprint_sha256"],
        fingerprints_verified=True, warm_up_queries=warm, total_queries=index,
        original_counters=stats,
        per_forwarder_allocation=allocation,
        request_history={model: dict(row) for model, row in origins.items()},
        non_ttl_repeat_certificates={model: dict(row) for model, row in fresh_repeat.items()},
        by_ttl={str(key): dict(value) for key, value in sorted(by_ttl.items())},
        paired_transitions=dict(sorted(transitions.items())),
        minute_buckets={str(key): dict(value) for key, value in sorted(time_buckets.items())},
        wall_latency_by_planned_second_phase={str(key): dict(value)
                                             for key, value in sorted(wall_second_phase.items())},
        formal_domain_demand=dict(
            distinct_domains=len(domain_demand),
            one_request_domains=sum(count == 1 for count in domain_demand.values()),
            two_request_domains=sum(count == 2 for count in domain_demand.values())),
        advice_funnel=dict(
            advice_runs=manager["prefetch_advice_runs"],
            candidates=manager["prefetch_advised"], assigned=manager["prefetch_tasks_sent"],
            started=fw["prefetch_started"], skipped=fw["prefetch_skipped"],
            used_at_landing=fw["prefetch_records_used"],
            admission_fraction=manager["prefetch_tasks_sent"] / manager["prefetch_advised"],
            unassigned=manager["prefetch_advised"] - manager["prefetch_tasks_sent"],
            total_shard_budget_per_round=cfg["num_forwarders"] *
                                        cfg["prefetch"]["max_per_forwarder"] // cfg["workers"]["manager"],
        ),
        counter_deltas={layer: {key: prefetch[layer][key] - plain[layer][key]
                               for key in plain[layer] if isinstance(plain[layer][key], (int, float))}
                        for layer in ("client", "forwarder", "manager")},
        limitations=[
            "Histories use all client requests, not the subset observed by forwarders.",
            "No prefetch task/eviction lifetimes were logged; no exact usefulness or pollution attribution.",
            "Paired transitions are observed outcomes, not isolated causal effects of prefetching.",
            "Non-TTL certificates cover only earlier formal origin fetches, using conservative time bounds.",
        ],
    )
    output = Path(__file__).with_name("audit_saved.json")
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in (
        "advice_funnel", "request_history", "non_ttl_repeat_certificates", "by_ttl",
        "paired_transitions", "formal_domain_demand")}, indent=2))


if __name__ == "__main__":
    main()
