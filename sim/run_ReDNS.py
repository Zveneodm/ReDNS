"""Run the ReDNS model with client, forwarder and authority processes."""

from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.experiment import run_experiment
from sim.realtime import RealtimeSimulation


class Simulation(RealtimeSimulation):
    def __init__(self, config):
        super().__init__(config, "ReDNS")


def run_simulation(config):
    return Simulation(config).run()


def main(argv=None):
    return run_experiment(run_simulation, "ReDNS", "ReDNS_results.json", argv)


if __name__ == "__main__":
    raise SystemExit(main())
