"""Command-line entry points and streaming experiment result output."""

import argparse
from array import array
import json
from pathlib import Path

from lib.config import PROJECT_ROOT, load_config, validate_config
from src.variants import PREFETCH_MODELS
from lib.results import allocate_result_directory

def write_results(path, results):
    """Stream large per-request arrays without making a second full list."""
    with Path(path).open("w", encoding="utf-8") as stream:
        stream.write("{\n")
        for index, (key, value) in enumerate(results.items()):
            if index:
                stream.write(",\n")
            stream.write("  " + json.dumps(key) + ": ")
            if isinstance(value, array):
                stream.write("[")
                for start in range(0, len(value), 8192):
                    if start:
                        stream.write(",")
                    stream.write("\n    " + json.dumps(value[start:start + 8192].tolist())[1:-1])
                stream.write("\n  ]" if value else "]")
            else:
                stream.write(json.dumps(value, ensure_ascii=False, indent=2).replace("\n", "\n  "))
        stream.write("\n}\n")


def _time_scale(value):
    return "auto" if value == "auto" else float(value)


def run_experiment(run_simulation, label, argv=None):
    parser = argparse.ArgumentParser(description=f"Run the {label} multiprocessing DNS experiment")
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / "config.json")
    parser.add_argument("--output", type=Path, help="Override the JSON results path")
    parser.add_argument("--replacement-policy", choices=("lru", "random"))
    parser.add_argument("--time-scale", type=_time_scale,
                        help="Wall seconds per simulated second, or 'auto'")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.replacement_policy is not None:
            config["replacement_policy"] = args.replacement_policy
        if args.time_scale is not None:
            config["time_scale"] = args.time_scale
        config = validate_config(config)
        results = run_simulation(config)
        if not results["timing_valid"]:
            raise RuntimeError("arrival backlog invalidated this run; results were not saved. "
                               "Use python3 -m sim.run_comparison to select a common slower rate. "
                               + "; ".join(results["timing_validation"]["reasons"]))
        output = args.output or (allocate_result_directory(PROJECT_ROOT / config["output_dir"])
                                / f"{results['experiment']}_results.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        write_results(output, results)
        if args.output is None:
            write_results(output.parent / "config.json", config)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    print(f"{results['experiment']}: {results['total_queries']} queries over {results['elapsed_seconds']} simulated seconds")
    for layer in ("client", "forwarder"):
        stats = results[layer]
        print(f"{layer}: queries={stats['queries']}, hits={stats['hits']}, "
              f"misses={stats['misses']}, hit_rate={stats['hit_rate']:.2%}")
    print(f"authoritative_queries={results['authoritative_queries']}")
    if results["experiment"] in PREFETCH_MODELS:
        budget = results["manager"]
        print(f"prefetch_budget_ratio={config['prefetch']['budget_ratio']:g}, "
              f"budget_demand={budget['prefetch_budget_demand']}, "
              f"budget_allowance={budget['prefetch_budget_allowance']}, "
              f"prefetch_queries={results['prefetch_authoritative_queries']}")
    print(f"successful_reverse_queries={results['successful_reverse_queries']}")
    print(f"manager_queries={results['manager_queries']}, "
          f"manager_directory_hits={results['manager_directory_hits']}, "
          f"manager_directory_misses={results['manager_directory_misses']}")
    print(f"cluster_queries={results['cluster_queries']}, cluster_hits={results['cluster_hits']}, "
          f"cluster_forwarder_hits={results['cluster_forwarder_hits']}, "
          f"cluster_client_hits={results['cluster_client_hits']}")
    print(f"average_latency_ms={results['average_latency_ms']:.6f}")
    print(f"average_wall_latency_ms={results['average_wall_latency_ms']:.6f}")
    print(f"actual_time_scale={results['actual_time_scale']:.9g}")
    print(f"arrival_lag_mean_sim_seconds={results['arrival_lag_mean_sim_seconds']:.6f}")
    print(f"Results: {output.resolve()}")
    return 0
