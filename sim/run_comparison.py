"""Run and publish a matched baseline/ReDNS comparison."""

import argparse
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.comparison import run_comparison
from lib.config import PROJECT_ROOT, load_config
from lib.experiment import _time_scale


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / "config.json")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--time-scale", type=_time_scale)
    parser.add_argument("--replacement-policy", choices=("lru", "random"))
    models = parser.add_mutually_exclusive_group()
    models.add_argument("--include-prefetch", action="store_true",
                        help="Compare baseline, ReDNS and ReDNS-prefetch together")
    models.add_argument("--prefetch-ablation", action="store_true",
                        help="Compare baseline-prefetch with ReDNS-prefetch using the same prefetch policy")
    args = parser.parse_args(argv)
    try:
        cfg = load_config(args.config)
        if args.time_scale is not None:
            cfg["time_scale"] = args.time_scale
        if args.replacement_policy is not None:
            cfg["replacement_policy"] = args.replacement_policy
        manifest = run_comparison(cfg, args.output_dir, include_prefetch=args.include_prefetch,
                                  prefetch_ablation=args.prefetch_ablation)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    print(f"Accepted common time_scale={manifest['actual_time_scale']:g}")
    for model, row in manifest["models"].items():
        print(f"{model}: queries={row['total_queries']}, "
              f"average_latency_ms={row['average_latency_ms']:.6f}, "
              f"mean_arrival_lag_seconds={row['timing_validation']['mean_lag_seconds']:.6f}")
    print(f"Comparison: {Path(manifest['result_directory']) / 'comparison.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
