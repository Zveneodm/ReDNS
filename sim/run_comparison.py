"""Run and publish a matched baseline/ReDNS comparison."""

import argparse
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.comparison import run_comparison
from sim.config import PROJECT_ROOT, load_config
from sim.experiment import _time_scale


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / "config.json")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--time-scale", type=_time_scale)
    parser.add_argument("--replacement-policy", choices=("lru", "random"))
    args = parser.parse_args(argv)
    try:
        cfg = load_config(args.config)
        if args.time_scale is not None:
            cfg["time_scale"] = args.time_scale
        if args.replacement_policy is not None:
            cfg["replacement_policy"] = args.replacement_policy
        output = args.output_dir or PROJECT_ROOT / cfg["output_dir"] / "comparison"
        manifest = run_comparison(cfg, output)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    print(f"Accepted common time_scale={manifest['actual_time_scale']:g}")
    for model, row in manifest["models"].items():
        print(f"{model}: queries={row['total_queries']}, "
              f"average_latency_ms={row['average_latency_ms']:.6f}, "
              f"mean_arrival_lag_seconds={row['timing_validation']['mean_lag_seconds']:.6f}")
    print(f"Comparison: {(output / 'comparison.json').resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
