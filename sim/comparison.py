"""Paired trials with one clock scale, identical inputs, and a backlog gate."""

from copy import deepcopy
from hashlib import sha256
from math import ceil
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from sim.config import PROJECT_ROOT, validate_config
from sim.experiment import write_results
from sim.realtime import ROLES, RealtimeSimulation, available_cpu_pool
from sim.timing import SimulationOverload


MODELS = ("baseline", "ReDNS")
PREFETCH_MODELS = (*MODELS, "ReDNS-prefetch")


def source_fingerprint():
    """Detect a source edit between paired runs, including uncommitted changes."""
    digest = sha256()
    for folder in ("src", "sim"):
        for path in sorted((PROJECT_ROOT / folder).rglob("*.py")):
            digest.update(path.relative_to(PROJECT_ROOT).as_posix().encode() + b"\0")
            digest.update(path.read_bytes())
    return digest.hexdigest()


def _run(config, model, cpu_pool):
    return RealtimeSimulation(config, model, cpu_pool=cpu_pool, abort_on_overload=True).run()


def _summary(result):
    fields = ("experiment", "config", "actual_time_scale", "cpu_pool", "workers",
              "workload_fingerprint_sha256", "ttl_fingerprint_sha256",
              "warm_up_queries", "total_queries", "timing_valid", "timing_validation",
              "observed_simulated_seconds", "average_latency_ms", "average_wall_latency_ms",
              "authoritative_queries", "successful_reverse_queries", "manager_queries",
              "cluster_hits", "wall_run_seconds")
    row = {key: result[key] for key in fields}
    for key in ("prefetch_authoritative_queries", "authoritative_queries_total"):
        if key in result:
            row[key] = result[key]
    if "forwarder" in result:
        row["prefetch"] = {key: value for key, value in result["forwarder"].items()
                           if key.startswith("prefetch_")}
    if "manager" in result:
        row["prefetch_budget"] = {key: value for key, value in result["manager"].items()
                                  if key.startswith("prefetch_budget_")}
    return row


def validate_pair(rows, expected_config, cpu_pool):
    """Accept a pair only when its timing and controlled inputs match exactly."""
    first = rows["baseline"]
    for model, row in rows.items():
        if not row["timing_valid"]:
            raise SimulationOverload(row["timing_validation"])
        if row["experiment"] != model or row["config"] != expected_config:
            raise RuntimeError("paired run model/config does not match the requested experiment")
        if row["actual_time_scale"] != expected_config["time_scale"]:
            raise RuntimeError("paired run changed its clock scale")
        if row["cpu_pool"] != list(cpu_pool):
            raise RuntimeError("paired run changed its CPU pool")
    public = {name for name in first["workers"]
              if first["workers"][name].get("role", name) in (*ROLES, "driver")}
    for second in rows.values():
        for field in ("workload_fingerprint_sha256", "ttl_fingerprint_sha256",
                      "warm_up_queries", "total_queries"):
            if first[field] != second[field]:
                raise RuntimeError(f"paired inputs differ: {field}")
        other = {name for name in second["workers"]
                 if second["workers"][name].get("role", name) in (*ROLES, "driver")}
        if public != other:
            raise RuntimeError("paired common worker ownership differs")
        for name in public:
            if first["workers"][name]["cpu"] != second["workers"][name]["cpu"]:
                raise RuntimeError(f"paired CPU binding differs: {name}")
            for field in ("role", "shard", "shards", "node_count"):
                if first["workers"][name].get(field) != second["workers"][name].get(field):
                    raise RuntimeError(f"paired worker ownership differs: {name}.{field}")


def run_comparison(config, output_dir, *, runner=None, include_prefetch=False):
    """Probe both full protocols, then rerun both from empty caches at one rate.

    A failed formal trial invalidates the whole pair. The scale changes only
    between attempts. Each accepted pair is published as a complete directory;
    comparison.json is an atomic pointer to that pair. Older pairs remain intact.
    """
    config = validate_config(config)
    models = PREFETCH_MODELS if include_prefetch else MODELS
    limits = config["timing"]
    scale = (limits["auto_initial_time_scale"] if config["time_scale"] == "auto"
             else config["time_scale"])
    scale = max(scale, config["min_time_scale"])
    cpu_pool = available_cpu_pool(config)
    source_hash = source_fingerprint()
    runner = runner or _run
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    history = []

    def trial(cfg, model):
        result = runner(deepcopy(cfg), model, cpu_pool)
        if source_fingerprint() != source_hash:
            raise RuntimeError("simulation source changed during the comparison; discard this pair")
        if not result["timing_valid"]:
            raise SimulationOverload(result["timing_validation"])
        return result

    for attempt in range(1, limits["max_attempts"] + 1):
        cfg = validate_config({**config, "time_scale": scale})
        probe = deepcopy(cfg)
        probe["warm_up_seconds"] = min(cfg["warm_up_seconds"], limits["probe_warm_up_seconds"])
        minimum_sample_seconds = (ceil(cfg["calibration_requests"] / cfg["queries_per_second"])
                                  if cfg["queries_per_second"] else 1)
        probe["duration_seconds"] = min(cfg["duration_seconds"],
                                        max(limits["probe_duration_seconds"], minimum_sample_seconds))
        entry = dict(attempt=attempt, time_scale=scale, probes={}, runs={})
        history.append(entry)
        print(f"Comparison attempt {attempt}: common time_scale={scale:g}", flush=True)
        # Each pilot uses real cache fills, evictions, notifications and manager
        # traffic. A fast path with cache filling disabled cannot estimate this.
        probe_failed = False
        probe_rows = {}
        for model in models:
            print(f"Full-protocol pilot: {model}", flush=True)
            try:
                result = trial(probe, model)
                probe_rows[model] = _summary(result)
                entry["probes"][model] = probe_rows[model]
                del result
            except SimulationOverload as exc:
                entry["probes"][model] = dict(timing_valid=False, timing_validation=exc.report)
                print(f"Discard {model} pilot: {'; '.join(exc.report['reasons'])}", flush=True)
                probe_failed = True
        if probe_failed:
            scale *= limits["scale_multiplier"]
            continue
        validate_pair(probe_rows, probe, cpu_pool)

        with TemporaryDirectory(prefix=".trial-", dir=output_dir) as temporary:
            stage = Path(temporary)
            try:
                for model in models:
                    print(f"Fresh formal run: {model}", flush=True)
                    result = trial(cfg, model)
                    entry["runs"][model] = _summary(result)
                    write_results(stage / f"{model}_results.json", result)
                    del result
                validate_pair(entry["runs"], cfg, cpu_pool)
            except SimulationOverload as exc:
                entry["failure"] = exc.report
                print(f"Discard entire pair: {'; '.join(exc.report['reasons'])}", flush=True)
                # Temporary files disappear together; an earlier baseline result
                # must never be paired with a slower ReDNS retry.
                scale *= limits["scale_multiplier"]
                continue
            run_name = "run-" + uuid4().hex[:12]
            manifest = dict(schema_version=1, timing_valid=True, common_config=cfg,
                            actual_time_scale=scale, cpu_pool=list(cpu_pool),
                            source_fingerprint_sha256=source_hash, attempts=history,
                            results={model: f"{run_name}/{model}_results.json" for model in models},
                            models=entry["runs"])
            write_results(stage / "manifest.json", manifest)
            os.replace(stage, output_dir / run_name)
        # Publish only after both complete files and their manifest exist.
        pointer = output_dir / f".comparison-{uuid4().hex}.json"
        try:
            write_results(pointer, manifest)
            os.replace(pointer, output_dir / "comparison.json")
        finally:
            pointer.unlink(missing_ok=True)
        return manifest
    raise RuntimeError(f"no sustainable common rate after {limits['max_attempts']} attempts; "
                       "increase time_scale or timing.max_attempts. No new pair was published.")
