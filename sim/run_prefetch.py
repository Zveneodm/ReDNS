"""Run the ReDNS model with cluster-managed prefetching."""

from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.experiment import run_experiment
from sim.realtime import RealtimeSimulation


def run_simulation(config):
    return RealtimeSimulation(config, "ReDNS-prefetch").run()


def main(argv=None):
    return run_experiment(run_simulation, "ReDNS-prefetch", "ReDNS-prefetch_results.json", argv)


if __name__ == "__main__":
    raise SystemExit(main())
