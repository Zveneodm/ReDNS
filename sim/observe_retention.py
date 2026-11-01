"""Run optional cache observations and compare popularity with eviction events."""

import argparse
from array import array
from collections import Counter
import gzip
import json
from pathlib import Path
import subprocess
import sys

from sim.arrivals import Workload
from sim.config import PROJECT_ROOT, load_config, validate_config
from sim.experiment import write_results
from sim.realtime import RealtimeSimulation
from sim.retention import FIELDS, INDEX


def offered_popularity(config):
    """Replay only the seeded generator; no truth is fed into either model."""
    workload = Workload(config)
    regions = [Counter() for _ in range(config["num_forwarders"])]
    client_totals = array("I", [0]) * config["num_clients"]
    total = 0
    end = config["warm_up_seconds"] + config["duration_seconds"]
    while workload.next_time < end:
        when, client, domain = workload.pop()
        client_totals[client] += 1
        if when >= config["warm_up_seconds"]:
            regions[client % config["num_forwarders"]][domain] += 1
            total += 1
    return regions, {"formal_queries": total, "max_total_queries_per_client": max(client_totals),
                     "mean_total_queries_per_client": sum(client_totals) / len(client_totals)}


def read_profile(path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        profile = json.load(stream)
    if tuple(profile["fields"]) != FIELDS:
        raise ValueError("unexpected retention fields")
    return profile, {int(node): {row[0]: row[1:] for row in rows}
                     for node, rows in profile["rows"].items()}


def summarize_groups(profile, rows, rankings):
    """Cumulative hot sets are ranked independently within each cache's scope."""
    span = profile["end"] - profile["start"]
    totals = {name: {"pairs": 0, "demand_queries": 0, "pairs_ever_cached": 0,
                     "pairs_ever_capacity_evicted": 0, **dict.fromkeys(FIELDS, 0)}
              for name in ("top_1", "top_10", "top_100", "top_1000", "outside_top_1000")}
    top_examples = []
    for node_id, cache_rows in rows.items():
        demand = rankings[node_id]
        ranked = sorted(demand, key=lambda domain: (-demand[domain], domain))
        groups = {f"top_{limit}": ranked[:limit] for limit in (1, 10, 100, 1000)}
        groups["outside_top_1000"] = sorted((cache_rows.keys() | demand.keys()) - set(ranked[:1000]))
        for name, domains in groups.items():
            group = totals[name]
            group["pairs"] += len(domains)
            group["demand_queries"] += sum(demand.get(domain, 0) for domain in domains)
            for domain in domains:
                row = cache_rows.get(domain)
                if row is None:
                    continue
                group["pairs_ever_cached"] += int(row[INDEX["episodes"]] > 0)
                group["pairs_ever_capacity_evicted"] += int(row[INDEX["capacity_evictions"]] > 0)
                for field, value in zip(FIELDS, row):
                    group[field] += value
        for rank, domain in enumerate(ranked[:10], 1):
            top_examples.append({"node_id": node_id, "domain": domain, "rank": rank,
                                 "demand_queries": demand[domain],
                                 **dict(zip(FIELDS, cache_rows.get(domain, [0] * len(FIELDS))))})
    for group in totals.values():
        group["capacity_evictions_per_episode"] = (group["capacity_evictions"] / group["episodes"]
                                                   if group["episodes"] else 0.0)
        group["pair_evicted_fraction"] = (group["pairs_ever_capacity_evicted"] / group["pairs"]
                                           if group["pairs"] else 0.0)
        group["mean_live_presence"] = (group["live_seconds"] / (group["pairs"] * span)
                                        if group["pairs"] and span else 0.0)
        group["local_hit_rate"] = group["hits"] / group["queries"] if group["queries"] else 0.0
    return {"groups": totals, "top_examples": top_examples}


def analyze(directory, models):
    config = load_config(directory / "config.json")
    print("Replaying offered arrivals to rank regional demand...", flush=True)
    offered, workload = offered_popularity(config)
    summary = {"config": config, "workload": workload, "runs": {}}
    for model in models:
        with (directory / f"{model}_results.json").open() as stream:
            result = json.load(stream)
        if result["total_queries"] != workload["formal_queries"]:
            raise RuntimeError("observed and replayed request counts differ")
        run = {key: result[key] for key in (
            "total_queries", "authoritative_queries", "average_latency_ms", "average_wall_latency_ms",
            "arrival_lag_mean_sim_seconds", "arrival_lag_max_wall_ms", "observed_simulated_seconds",
            "actual_time_scale", "client", "forwarder", "workers",
        )}
        for field in ("successful_reverse_queries", "cluster_queries", "cluster_hits",
                      "cluster_forwarder_hits", "cluster_client_hits", "manager_enabled",
                      "manager_queries", "manager_directory_hits", "manager_directory_misses", "manager"):
            if field in result:
                run[field] = result[field]
        run["max_client_cache_entries_at_end"] = max(row["cache_entries"] for row in result["clients"])
        run["mean_client_cache_entries_at_end"] = result["client"]["cache_entries"] / config["num_clients"]
        run["max_forwarder_cache_entries_at_end"] = max(row["cache_entries"] for row in result["forwarders"])
        for role in ("client", "forwarder"):
            profile, rows = read_profile(directory / f"{model}_{role}_retention.json.gz")
            visible = {node: {domain: row[INDEX["queries"]] for domain, row in node_rows.items()
                              if row[INDEX["queries"]] > 0} for node, node_rows in rows.items()}
            run[f"{role}_visible_popularity"] = summarize_groups(profile, rows, visible)
            run[f"{role}_observation"] = {key: profile[key] for key in ("start", "end", "node_ids")}
            if role == "forwarder":
                run["forwarder_regional_popularity"] = summarize_groups(profile, rows, dict(enumerate(offered)))
        summary["runs"][model] = run
    write_results(directory / "summary.json", summary)
    write_report(directory, summary)
    return summary


def write_report(directory, summary):
    cfg = summary["config"]
    lines = [
        "# 热门域名缓存留存观察", "",
        f"负载：{cfg['num_clients']:,} client、{cfg['num_forwarders']} forwarder、"
        f"{cfg['queries_per_second']:,} 请求/模拟秒；缓存容量均按 config.json；替换策略 {cfg['replacement_policy']}。",
        f"预热 {cfg['warm_up_seconds']} 秒，正式 {cfg['duration_seconds']} 秒，"
        f"固定 time_scale={cfg['time_scale']}。三进程实际运行，未改变缓存、转存或 TTL 规则。", "",
        "正常查询热度按计划正式窗口计数；驻留时间和退出事件按实际模拟时钟、从预热边界到最终快照观察。"
        "逐出仅指容量替换；TTL 逻辑到期单列，包含尚未物理清理的到期条目。", "",
        "热门集合分别在每个 forwarder 内排名，不把域名编号当作全局热度。"
        "`forwarder 可见` 指实际穿过 client 缓存、到达 forwarder 的查询；"
        "`区域用户需求` 包含被 client 缓存接住的请求，由同一 seed 的负载离线重放得到。", "",
        "| 模型 | 热度口径 | 每节点热门集合 | 域名-节点对数 | 缓存版本数 | 容量逐出数 | 逐出/版本 | TTL 到期数 | 平均有效驻留比例 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for model, run in summary["runs"].items():
        for scope, label in (("forwarder_visible_popularity", "forwarder 可见"),
                             ("forwarder_regional_popularity", "区域用户需求"),
                             ("client_visible_popularity", "抽样 client 自身需求")):
            for band in ("top_1", "top_10", "top_100", "top_1000", "outside_top_1000"):
                group = run[scope]["groups"][band]
                lines.append(f"| {model} | {label} | {band} | {group['pairs']:,} | {group['episodes']:,} | "
                             f"{group['capacity_evictions']:,} | {group['capacity_evictions_per_episode']:.2%} | "
                             f"{group['ttl_expirations']:,} | {group['mean_live_presence']:.2%} |")
    lines.extend(["", "缓存版本指一次实际存入到删除/替换/快照的有效生命周期，包含预热时已存在、"
                  "正式窗口内仍有效的版本。逐出/版本不是每个域名的逐出概率；"
                  "`pairs_ever_capacity_evicted` 另存于 summary.json。各 top 集合累计包含前面的集合，不能相加。", ""])
    for model, run in summary["runs"].items():
        lines.extend([
            f"## {model}", "",
            f"正式查询 {run['total_queries']:,}；client 容量逐出 "
            f"{run['client']['lru_evictions'] + run['client']['random_evictions']:,}；"
            f"forwarder 容量逐出 {run['forwarder']['lru_evictions'] + run['forwarder']['random_evictions']:,}。",
            f"结束时每 client 平均物理条目 {run['mean_client_cache_entries_at_end']:.2f}，"
            f"单 client 最大 {run['max_client_cache_entries_at_end']}；转存发送 {run['forwarder']['records_transferred']:,}。",
            f"平均到达滞后 {run['arrival_lag_mean_sim_seconds']:.6f} 模拟秒；"
            f"实际流逝 {run['observed_simulated_seconds']:.6f} 模拟秒。", "",
        ])
        if "successful_reverse_queries" in run:
            lines.extend([f"成功反向查询 {run['successful_reverse_queries']:,} 次："
                          "forwarder 未命中后，从请求方之外的另一个 client 实际取回缓存记录。", ""])
    lines.extend([
        f"整个预热+正式负载中，每 client 最多收到 {summary['workload']['max_total_queries_per_client']} 次正常请求。", "",
        "观察器覆盖全部 forwarder，并对固定 client ID 样本记录按域名的生命周期；"
        "全体 client 的逐出和占用总计来自原有实验统计。观察器增加执行开销，"
        "该实验用于观察缓存行为，不将观察器开启时的进程耗时当作模型性能基准。", "",
        "原始域名级数据保存为 *_retention.json.gz，配置及原始实验结果保存在同目录。", "",
    ])
    (directory / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / "config.json")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--warm-up-seconds", type=int)
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--time-scale", type=float, default=1.0)
    parser.add_argument("--sample-clients", type=int, default=300)
    parser.add_argument("--model", choices=("baseline", "ReDNS", "both"), default="both")
    parser.add_argument("--analyze-only", action="store_true")
    args = parser.parse_args(argv)
    directory = args.output_dir.resolve()
    models = ("baseline", "ReDNS") if args.model == "both" else (args.model,)
    if args.analyze_only:
        analyze(directory, models)
        return 0
    if args.sample_clients < 0:
        parser.error("sample-clients must be non-negative")
    config = load_config(args.config)
    for field, value in (("warm_up_seconds", args.warm_up_seconds),
                         ("duration_seconds", args.duration_seconds)):
        if value is not None:
            config[field] = value
    config.update(time_scale=args.time_scale, record_wall_latencies=False)
    config = validate_config(config)
    directory.mkdir(parents=True, exist_ok=True)
    write_results(directory / "config.json", config)
    for model in models:
        print(f"Observing {model}...", flush=True)
        result = RealtimeSimulation(config, model, observation={
            "output_dir": str(directory), "sample_clients": args.sample_clients,
        }).run()
        write_results(directory / f"{model}_results.json", result)
        print(f"{model}: queries={result['total_queries']:,}; "
              f"successful_reverse_queries={result['successful_reverse_queries']:,}; "
              f"arrival_lag={result['arrival_lag_mean_sim_seconds']:.6f}s", flush=True)
        del result
    # Analysis consumes only saved artifacts. Keep it in a fresh interpreter,
    # separate from the multiprocessing coordinator and its large result arrays.
    subprocess.run([
        sys.executable, "-m", "sim.observe_retention", "--output-dir", str(directory),
        "--model", args.model, "--analyze-only",
    ], cwd=PROJECT_ROOT, check=True)
    print(f"Report: {directory / 'report.md'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
