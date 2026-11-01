"""Summarize prefetch comparisons, including cache-cooperation ablations."""

import argparse
from collections import Counter
import json
from math import ceil
from pathlib import Path


def percentile(histogram, fraction):
    target = ceil(sum(histogram.values()) * fraction)
    count = 0
    for value, frequency in sorted(histogram.items()):
        count += frequency
        if count >= target:
            return value
    return 0.0


def summarize(directory):
    manifest = json.loads((directory / "comparison.json").read_text())
    summaries = {}
    for model, relative in manifest["results"].items():
        result = json.loads((directory / relative).read_text())
        histogram = Counter(result["request_latencies_ms"])
        count = result["total_queries"]
        summaries[model] = dict(
            total_queries=count, average_latency_ms=result["average_latency_ms"],
            p50_latency_ms=percentile(histogram, 0.50), p95_latency_ms=percentile(histogram, 0.95),
            p99_latency_ms=percentile(histogram, 0.99),
            average_wall_latency_ms=result["average_wall_latency_ms"],
            foreground_authoritative_queries=result["authoritative_queries"],
            prefetch_authoritative_queries=result["prefetch_authoritative_queries"],
            total_authoritative_queries=result["authoritative_queries_total"],
            foreground_origin_fraction=result["authoritative_queries"] / count if count else 0,
            client_hit_rate=result["client"]["hit_rate"],
            forwarder_hit_rate=result["forwarder"]["hit_rate"],
            cluster_hits=result["cluster_hits"],
            prefetch_records_used=result["forwarder"]["prefetch_records_used"],
            prefetch_stored=result["forwarder"]["prefetch_stored"],
            prefetch_skipped=result["forwarder"]["prefetch_skipped"],
            prefetch_use_fraction=(result["forwarder"]["prefetch_records_used"] /
                                   result["forwarder"]["prefetch_stored"]
                                   if result["forwarder"]["prefetch_stored"] else 0),
            authority_queries_per_request=result["authoritative_queries_total"] / count if count else 0,
            successful_reverse_queries=result["successful_reverse_queries"],
            coalesced_queries=result["forwarder"].get("coalesced_queries", 0),
            cooperation_enabled=result.get("cooperation_enabled", model.startswith("ReDNS")),
            prefetch_enabled=result.get("prefetch_enabled", model.endswith("-prefetch")),
            timing_valid=result["timing_valid"],
            mean_lag_seconds=result["timing_validation"]["mean_lag_seconds"],
            max_lag_seconds=result["timing_validation"]["max_lag_seconds"],
            overrun_seconds=result["timing_validation"]["overrun_seconds"],
            ipc_messages=result["ipc_messages_sent"],
            prefetch_budget={key: value for key, value in result["manager"].items()
                             if key.startswith("prefetch_budget_")},
        )
        del result
    return manifest, summaries


def reduction(reference, candidate):
    """A positive value means the candidate uses less latency or traffic."""
    return 1 - candidate / reference if reference else None


def compare(reference, candidate):
    fields = ("average_latency_ms", "foreground_authoritative_queries",
              "prefetch_authoritative_queries", "total_authoritative_queries")
    gains = {f"{field}_reduction": reduction(reference[field], candidate[field]) for field in fields}
    gains["prefetch_use_fraction_change"] = candidate["prefetch_use_fraction"] - reference["prefetch_use_fraction"]
    return gains


def percentage(value):
    return f"{value:.3%}" if value is not None else "N/A（参考值为零）"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    manifest, rows = summarize(args.directory)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cfg = manifest["common_config"]
    first = next(iter(manifest["models"].values()))
    ablation = "baseline-prefetch" in rows and "ReDNS-prefetch" in rows
    comparisons = {}
    if ablation:
        comparisons["ReDNS-prefetch_vs_baseline-prefetch"] = compare(rows["baseline-prefetch"], rows["ReDNS-prefetch"])
    if "ReDNS" in rows and "ReDNS-prefetch" in rows:
        comparisons["ReDNS-prefetch_vs_ReDNS"] = compare(rows["ReDNS"], rows["ReDNS-prefetch"])
    summary = dict(common_config=cfg, actual_time_scale=manifest["actual_time_scale"],
                   comparison_kind=manifest.get("comparison_kind"), comparisons=comparisons,
                   source_fingerprint_sha256=manifest["source_fingerprint_sha256"],
                   workload_fingerprint_sha256=first["workload_fingerprint_sha256"],
                   ttl_fingerprint_sha256=first["ttl_fingerprint_sha256"],
                   results=manifest["results"], models=rows)
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = ["# 预取下的 ReDNS 协作消融对照" if ablation else "# ReDNS 集群预取正式对照", "",
             f"seed={cfg['seed']}；{cfg['num_clients']:,} client；{cfg['num_forwarders']} forwarder；"
             f"集群大小 {cfg['forwarder_cluster_size']}；{cfg['queries_per_second']:,} 请求/模拟秒。",
             f"预热 {cfg['warm_up_seconds']} 秒，正式窗口 {cfg['duration_seconds']} 秒；"
             f"共同 time_scale={manifest['actual_time_scale']}。", "",
             "| 模型 | 平均 RTT 延迟 ms | p95 ms | 前台回源 | 预取回源 | 总回源 | 前台回源比例 |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for model, row in rows.items():
        lines.append(f"| {model} | {row['average_latency_ms']:.6f} | {row['p95_latency_ms']:.3f} | "
                     f"{row['foreground_authoritative_queries']:,} | {row['prefetch_authoritative_queries']:,} | "
                     f"{row['total_authoritative_queries']:,} | {row['foreground_origin_fraction']:.4%} |")
    if "ReDNS" in rows and "ReDNS-prefetch" in rows:
        prefetch, plain = rows["ReDNS-prefetch"], rows["ReDNS"]
        gains = comparisons["ReDNS-prefetch_vs_ReDNS"]
        lines += ["", f"相对无预取 ReDNS，平均 RTT 延迟降低 {percentage(gains['average_latency_ms_reduction'])}，"
                  f"总回源降低 {percentage(gains['total_authoritative_queries_reduction'])}。",
                  f"正式阶段预取存入 {prefetch['prefetch_stored']:,} 条，其中在观察结束前被实际查询使用 "
                  f"{prefetch['prefetch_records_used']:,} 条；跳过 {prefetch['prefetch_skipped']:,} 个任务。"]
    if ablation:
        gains = comparisons["ReDNS-prefetch_vs_baseline-prefetch"]
        lines += ["", "## 开启相同预取策略后，ReDNS 的额外收益", "",
                  f"相对 baseline-prefetch，ReDNS-prefetch 的平均 RTT 延迟降低 "
                  f"{percentage(gains['average_latency_ms_reduction'])}，前台回源降低 "
                  f"{percentage(gains['foreground_authoritative_queries_reduction'])}，总回源降低 "
                  f"{percentage(gains['total_authoritative_queries_reduction'])}。", "",
                  "| 模型 | 预取存入 | 已使用预取记录 | 使用比例 | 每用户请求总权威查询 | 成功反向查询 | 集群命中 |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for model, row in rows.items():
            lines.append(f"| {model} | {row['prefetch_stored']:,} | {row['prefetch_records_used']:,} | "
                         f"{row['prefetch_use_fraction']:.3%} | {row['authority_queries_per_request']:.6f} | "
                         f"{row['successful_reverse_queries']:,} | {row['cluster_hits']:,} |")
        lines += ["", "两组共用集群热度算法、预算比例、低负载派发、缓存容量、TTL、并发 miss 合并和请求序列。",
                  "baseline-prefetch 的 manager 仅执行预取，不启用副本目录、区域发现、邻居取回或淘汰转存；",
                  "预取结果只能由落点 forwarder 的请求使用。ReDNS-prefetch 可通过副本及区域目录共享答案。",
                  "这项消融衡量在当前集群预取策略上增加 ReDNS 整套协作机制的效果，不是所有独立 resolver 预取策略的上限。",
                  "预算按各模型实际到达 forwarder 的请求产生，相同比例不意味着相同的预取次数；总回源包含后台成本。",
                  "预取使用比例为正式阶段存入记录中至少在落点被使用一次的比例，观察末尾尚有效、未使用的记录不一定浪费；",
                  "逐出并转存后的后续使用由转存指标跟踪，不追记到原预取记录的使用数。"]
    lines += ["", "各组均从空缓存独立运行；工作负载与权威 TTL 指纹、公共进程所有权与 CPU 分配完全一致。",
              "每组通过到达积压检查、排空检查和流量守恒校验后才发布。", "",
              "这是单 seed 的完整实验，不提供跨重复试验的置信区间。请求来自项目现有的合成 Zipf/区域偏好模型。",
              "延迟为现有模拟器累计的配置 RTT；IPC 实耗单列，未模拟后台消息带宽或网络排队。",
              "预取按真实单调模拟时钟触发，统计与回填经过真实多进程消息队列；结果存在进程调度波动。", "",
              "算法是 Forestall 思路的集群适配，详见仓库 PREFETCH.md；不将该结果表述为论文原实现复现。",
              "参考论文：[Ferreira and Oki, Forestall (2026)](https://doi.org/10.1109/TNSM.2026.3704549)。", "",
              f"源代码 SHA-256：`{manifest['source_fingerprint_sha256']}`。",
              f"完整结果清单：`{args.directory / 'comparison.json'}`。", ""]
    (args.output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
