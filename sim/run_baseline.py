"""Run the Baseline model with client, forwarder and authority processes."""

from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.experiment import run_experiment
from lib.realtime import RealtimeSimulation
from lib.config import experiment_model


class Simulation(RealtimeSimulation):
    def __init__(self, config):
        super().__init__(config, experiment_model("baseline", config))


def run_simulation(config):
    return Simulation(config).run()


def main(argv=None):
    return run_experiment(run_simulation, "Baseline", argv)


if __name__ == "__main__":
    raise SystemExit(main())
