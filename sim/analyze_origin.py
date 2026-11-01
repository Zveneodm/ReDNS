"""Replay saved ReDNS arrivals to separate observed origin traffic from references.

The request history and authority classification can be recovered exactly after
fingerprint checks. Cache contents at historical instants were not saved. The
unbounded-cache replay therefore remains a reference, never a causal label for
the measured run or a strict lower bound on its formal-window origin count.
"""

import argparse
from array import array
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import random
from struct import pack
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.arrivals import Workload
from sim.config import PROJECT_ROOT, validate_config
from sim.experiment import write_results


HISTORY = ("first_global", "first_cluster_seen_elsewhere", "repeat_cluster")


def authority_threshold(result):
    """Prove that accumulated RTT separates authority calls in this saved run.

    Global failed-probe counters give a conservative bound on any single
    successful cache path, even if every failure preceded that success. Reject
    ambiguous RTT configurations rather than guessing from latency magnitude.
    """
    cfg, stats = result["config"], result["forwarder"]
    child = cfg["client_forwarder_RTT"]
    neighbor = cfg["forwarder_forwarder_RTT"]
    failed = stats["cluster_misses"] * neighbor + stats["cluster_child_misses"] * child
    maximum_cache_path = child + stats["peer_misses"] * child + max(
        child, cfg["forwarder_manager_RTT"] + failed + neighbor + child)
    minimum_authority_path = child + cfg["forwarder_authoritative_RTT"]
    if maximum_cache_path >= minimum_authority_path:
        raise ValueError("saved RTTs cannot unambiguously identify authority requests")
    return minimum_authority_path


def authority_ttls(config):
    """Reproduce the authority's initialization without querying any cache."""
    rng = random.Random(config["seed"] + 1)
    distribution = config["ttl_distribution"]
    if distribution is not None:
        return rng.choices(distribution["values"], weights=distribution["probabilities"],
                           k=config["num_domains"])
    return [rng.randint(*config["ttl_range"]) for _ in range(config["num_domains"])]


def workload_arrivals(config):
    workload = Workload(config)
    end = config["warm_up_seconds"] + config["duration_seconds"]
    while workload.next_time < end:
        yield workload.pop()


def analyze_trace(config, ttls, arrivals, latencies, *, threshold, progress=None):
    """Analyze one complete, ordered trace including warm-up.

    References fetch immediately on an uncovered request, keep all answers
    without capacity evictions, preserve the sampled TTL, and discover copies
    perfectly. Their clocks are planned arrival times, not observed wall times.
    """
    domains = config["num_domains"]
    size = config["forwarder_cluster_size"]
    clusters = (config["num_forwarders"] + size - 1) // size
    seen_cluster = bytearray(clusters * domains)
    seen_global = bytearray(domains)
    cluster_expiry = array("d", [0.0]) * (clusters * domains)
    global_expiry = array("d", [0.0]) * domains
    seen_origin_domain = bytearray(domains)
    histories = Counter(dict.fromkeys(HISTORY, 0))
    by_ttl = {}
    by_cluster = [Counter() for _ in range(clusters)]
    latency_histogram = Counter()
    reference = dict(cluster=Counter(), all_forwarders=Counter(),
                     actual_origin_against_cluster_reference=Counter(),
                     actual_cache_against_cluster_reference=Counter())
    fingerprint = sha256()
    index = warm = unique_global = unique_pairs = 0
    total = actual_origins = first_cluster_cache_hits = unique_origin_domains = 0
    previous_time = -1.0
    latency_iterator = iter(latencies)
    for when, client, domain in arrivals:
        if when < previous_time:
            raise ValueError("arrival trace is not ordered")
        previous_time = when
        fingerprint.update(pack("!dQQ", when, client, domain))
        forwarder = client % config["num_forwarders"]
        cluster = forwarder // size
        key = cluster * domains + domain
        first_global, first_cluster = not seen_global[domain], not seen_cluster[key]
        history = ("first_global" if first_global else
                   "first_cluster_seen_elsewhere" if first_cluster else "repeat_cluster")
        ttl = ttls[domain]
        local_reference_miss = cluster_expiry[key] <= when
        global_reference_miss = global_expiry[domain] <= when
        local_reference_reason = ("first_cluster" if first_cluster else "ttl_expired")
        global_reference_reason = ("first_global" if first_global else "ttl_expired")
        if local_reference_miss:
            cluster_expiry[key] = when + ttl
        if global_reference_miss:
            global_expiry[domain] = when + ttl
        seen_cluster[key] = seen_global[domain] = 1
        unique_global += first_global
        unique_pairs += first_cluster
        total += 1
        if when < config["warm_up_seconds"]:
            warm += 1
        else:
            try:
                latency = next(latency_iterator)
            except StopIteration as exc:
                raise ValueError("saved latency trace is shorter than the formal arrival trace") from exc
            index += 1
            is_origin = latency >= threshold
            by_cluster[cluster]["requests"] += 1
            histories[history] += 1
            if local_reference_miss:
                reference["cluster"]["origin_queries"] += 1
                reference["cluster"][local_reference_reason] += 1
            if global_reference_miss:
                reference["all_forwarders"]["origin_queries"] += 1
                reference["all_forwarders"][global_reference_reason] += 1
            comparison = ("actual_origin_against_cluster_reference" if is_origin
                          else "actual_cache_against_cluster_reference")
            reference[comparison][local_reference_reason if local_reference_miss else "cache_hit"] += 1
            if is_origin:
                actual_origins += 1
                latency_histogram[latency] += 1
                by_cluster[cluster]["origin_queries"] += 1
                by_cluster[cluster][history] += 1
                row = by_ttl.setdefault(ttl, Counter())
                row["origin_queries"] += 1
                row[history] += 1
                if not seen_origin_domain[domain]:
                    seen_origin_domain[domain] = 1
                    unique_origin_domains += 1
                    row["unique_domains"] += 1
            elif first_cluster:
                first_cluster_cache_hits += 1
        if progress is not None and total % 500_000 == 0:
            progress(total, when)
    sentinel = object()
    if next(latency_iterator, sentinel) is not sentinel:
        raise ValueError("saved latency trace is longer than the formal arrival trace")
    authority_history = {name: sum(row.get(name, 0) for row in by_ttl.values()) for name in HISTORY}
    return dict(
        schema_version=1, warm_up_queries=warm, total_queries=index,
        authoritative_queries=actual_origins, unique_origin_domains=unique_origin_domains,
        workload_fingerprint_sha256=fingerprint.hexdigest(),
        actual_origin_by_request_history=authority_history,
        actual_origin_by_ttl={str(ttl): dict(row) for ttl, row in sorted(by_ttl.items())},
        actual_origin_latency_histogram=dict(sorted(latency_histogram.items())),
        actual_origin_by_cluster=[dict(cluster_id=i, **row) for i, row in enumerate(by_cluster)],
        all_formal_requests_by_history=dict(histories),
        all_window_unique_domains=unique_global, all_window_unique_cluster_domain_pairs=unique_pairs,
        first_cluster_requests_without_origin=first_cluster_cache_hits,
        reference_replays={key: dict(value) for key, value in reference.items()},
        reference_assumptions=dict(capacity="unbounded", discovery="perfect",
                                   response="immediate", clock="planned_arrival_time",
                                   ttl="original_sampled_ttl_no_refresh_on_hit",
                                   includes_warm_up=True, strict_lower_bound=False),
    )


def analyze_saved_result(result, *, progress=None):
    if result["experiment"] != "ReDNS":
        raise ValueError("this analyzer expects a ReDNS result")
    config = validate_config(result["config"])
    ttls = authority_ttls(config)
    ttl_hash = sha256(json.dumps(ttls, separators=(",", ":")).encode()).hexdigest()
    if ttl_hash != result["ttl_fingerprint_sha256"]:
        raise ValueError("reconstructed authority TTL table does not match this run")
    threshold = authority_threshold(result)
    report = analyze_trace(config, ttls, workload_arrivals(config), result["request_latencies_ms"],
                           threshold=threshold, progress=progress)
    for field in ("workload_fingerprint_sha256", "total_queries", "warm_up_queries",
                  "authoritative_queries"):
        if report[field] != result[field]:
            raise ValueError(f"replay does not match saved result: {field}")
    report.update(config=config, ttl_fingerprint_sha256=ttl_hash,
                  fingerprints_verified=True, authority_latency_threshold_ms=threshold,
                  actual_formal_client_capacity_evictions=(result["client"]["lru_evictions"] +
                                                          result["client"]["random_evictions"]),
                  actual_formal_transfers=result["forwarder"]["records_transferred"])
    return report


def write_report(path, result):
    n = result["authoritative_queries"]
    labels = {"first_global": "全实验首次出现该域名",
              "first_cluster_seen_elsewhere": "本集群首次出现，其他集群此前已请求",
              "repeat_cluster": "本集群此前已经请求过"}
    lines = ["# ReDNS 权威查询拆解", "",
             f"正式权威查询共 **{n:,} 次**。请求序列、TTL 表、正式/预热请求数及权威查询数均已核对。", "",
             "## 可准确还原的请求历史", "",
             "历史从实验起点计算，包含预热；集群首次访问不是 client 首次访问。", "",
             "| 类别 | 次数 | 占权威查询 |", "| --- | ---: | ---: |"]
    for key, label in labels.items():
        count = result["actual_origin_by_request_history"][key]
        lines.append(f"| {label} | {count:,} | {count / n:.2%} |" if n else f"| {label} | 0 | 0% |")
    lines += ["", "跨集群先前有请求不保证当前还有有效副本。重复集群访问也不等于 TTL 过期。", "",
              "## 实际回源域名的权威 TTL", "",
              "| TTL（秒） | 回源次数 | 全局首次 | 集群首次、全局非首次 | 集群重复 |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for ttl, row in result["actual_origin_by_ttl"].items():
        lines.append(f"| {ttl} | {row['origin_queries']:,} | " +
                     " | ".join(f"{row.get(key, 0):,}" for key in HISTORY) + " |")
    refs = result["reference_replays"]
    lines += ["", "## 理想缓存重放参考", "",
              "使用相同的完整计划请求和原始 TTL，从空缓存开始并包含相同预热。参考缓存容量无限、",
              "发现即时且准确、响应即时，不主动刷新、不延长 TTL。它采用计划到达时间；原实验采用",
              "实际缩放壁钟，且响应存在处理时间，缓存历史会不同。参考值不是历史回源原因的实测计数，",
              "也不是原正式窗口的严格理论下界。", "",
              "| 参考查询范围 | 正式回源 | 范围内首次访问 | 参考缓存 TTL 已过期 |",
              "| --- | ---: | ---: | ---: |"]
    for key, label, cold in (("cluster", "原集群范围", "first_cluster"),
                             ("all_forwarders", "所有 forwarder 共享", "first_global")):
        row = refs[key]
        lines.append(f"| {label} | {row.get('origin_queries', 0):,} | {row.get(cold, 0):,} | "
                     f"{row.get('ttl_expired', 0):,} |")
    cross = refs["actual_origin_against_cluster_reference"]
    counterpart = refs["actual_cache_against_cluster_reference"]
    cluster_count = refs["cluster"].get("origin_queries", 0)
    global_count = refs["all_forwarders"].get("origin_queries", 0)
    repeat = result["actual_origin_by_request_history"]["repeat_cluster"]
    short_repeat = sum(row.get("repeat_cluster", 0) for ttl, row in
                       result["actual_origin_by_ttl"].items() if float(ttl) <= 60)
    lines += ["", "原实验实际回源请求，在原集群理想参考中对应：", "",
              f"- 范围内首次访问：{cross.get('first_cluster', 0):,} 次。",
              f"- 参考 TTL 已过期：{cross.get('ttl_expired', 0):,} 次。",
              f"- 参考缓存命中：{cross.get('cache_hit', 0):,} 次。", "",
              "最后一类只能作为诊断线索，不能直接标记为目录漏报、并发重复或容量逐出。",
              f"反向也有 {counterpart.get('ttl_expired', 0):,} 次：原实验缓存命中，参考重放却需要回源。",
              "这说明两种时钟和响应时刻会改变 TTL 相位，不能把参考与实测的差集直接当成实现错误。", "",
              "## 对优化方向的含义", "",
              f"原集群理想参考与实际的净差为 {n - cluster_count:,} 次。两者接近，结合正式 client",
              "容量逐出为零的事实，当前负载下增加容量或搬运相同到期版本的改善空间可能很小。",
              f"集群重复回源中，TTL 不超过 60 秒的有 {short_repeat:,} 次" +
              (f"，占 {short_repeat / repeat:.2%}。" if repeat else "。"),
              "重复访问类以短 TTL 域名为主，仍不能将所有重复访问直接标记为过期。", "",
              f"所有 forwarder 共享的理想参考为 {global_count:,} 次，比实测少 {n - global_count:,} 次" +
              (f"（{(n - global_count) / n:.2%}）。" if n else "。"),
              "它提示扩大可查询范围值得实测，但不是集群大小改为总数后必然取得的实际结果。",
              "建议保持请求序列与 TTL 不变，优先扫描集群大小，再评估维护成本。", "",
              "## 尚不能追溯的原因", "",
              "原文件未保存每次权威请求的发起/回复时刻、在途同域名请求、各缓存实际存删事件和",
              "目录查询时的真实副本状态，因此不能准确将既有回源分摊到 TTL 全部失效、有效副本逐出、",
              "目录漏报和并发重复。逐请求 RTT 只能识别回源路径，结束快照不能替代历史状态。", "",
              f"正式 client 容量逐出：{result['actual_formal_client_capacity_evictions']:,} 次；",
              f"正式转存发送：{result['actual_formal_transfers']:,} 次。", "",
              "完整机器可读统计见同目录 `origin_breakdown.json`。"]
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path,
                        default=PROJECT_ROOT / "res/comparison/comparison.json")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.comparison.read_text(encoding="utf-8"))
        source = args.comparison.parent / manifest["results"]["ReDNS"]
        print(f"Reading saved result: {source}", flush=True)
        with source.open(encoding="utf-8") as stream:
            saved = json.load(stream)
        # Service-time arrays and per-node rows are unnecessary for this replay.
        for field in ("request_wall_latencies_ms", "clients", "forwarders"):
            saved.pop(field, None)
        report = analyze_saved_result(saved, progress=lambda count, when:
                                      print(f"Offline replay: {count:,} requests, t={when:.1f}s", flush=True))
        report["source_result"] = str(source.resolve())
        output = args.output_dir or source.parent / "origin_analysis"
        output.mkdir(parents=True, exist_ok=True)
        write_results(output / "origin_breakdown.json", report)
        write_report(output / "report.md", report)
    except (OSError, ValueError, RuntimeError, KeyError) as exc:
        parser.error(str(exc))
    print(json.dumps(report["actual_origin_by_request_history"], ensure_ascii=False), flush=True)
    print(f"Report: {(output / 'report.md').resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
