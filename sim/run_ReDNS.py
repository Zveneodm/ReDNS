"""Run ReDNS with the same workload and clock as baseline."""

from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.experiment import run_experiment, validate_config
from sim.run_baseline import DeterministicSimulation as BaselineSimulation
from sim.realtime import RealtimeSimulation
from sim.preferences import PreferenceLearner
from src.Re_DNS.Model import ReDNS_Client, ReDNS_Forwarder, ReDNS_PublicDNSServer
from src.Re_DNS.Worker import Worker


class DeterministicSimulation(BaselineSimulation):
    """Reuse baseline traffic and reporting, substituting ReDNS model classes."""

    experiment_name = "ReDNS"
    public_server_class = ReDNS_PublicDNSServer
    forwarder_class = ReDNS_Forwarder
    client_class = ReDNS_Client

    def __init__(self, config):
        super().__init__(config)
        for resolver in self.resolvers:
            resolver.min_transfer_ttl = self.config["min_transfer_ttl"]
        options = self.config["regional_preferences"]
        if options["enabled"]:
            for resolver in [*self.forwarders, self.public_server]:
                resolver.preference_learner = PreferenceLearner(
                    options["learning_half_life_seconds"],
                    options["max_learned_domains_per_child"],
                )
            for child in [*self.clients, *self.forwarders]:
                parent = child.forwarder if hasattr(child, "forwarder") else child.pub_server
                original = child._on_cache_remove

                def removed(key, record, reason, child=child, parent=parent, original=original):
                    original(key, record, reason)
                    parent.preference_learner.forget_copy(
                        child.downstream_id, int(key[7:-8]),
                        self.clock.now + record.ttl,
                    )

                child._on_cache_remove = removed
        self.workers = [Worker(client) for client in self.clients]

    def results(self):
        result = super().results()
        if self.public_server.preference_learner is not None:
            result["public_dns_preference_learning"] = (
                self.public_server.preference_learner.metrics(self.clock.now))
            totals = [forwarder.preference_learner.metrics(self.clock.now)
                      for forwarder in self.forwarders]
            fitted = [row for row in totals if row["fitted_zipf_r2_mean"] > 0]
            result["forwarder_preference_learning"] = {
                "observed_requests": sum(row["observed_requests"] for row in totals),
                "tracked_children": sum(row["tracked_children"] for row in totals),
                "tracked_child_domain_pairs": sum(
                    row["tracked_child_domain_pairs"] for row in totals),
                "fitted_zipf_alpha_mean": (
                    sum(row["fitted_zipf_alpha_mean"] for row in fitted) / len(fitted)
                    if fitted else 0.0),
                "fitted_zipf_r2_mean": (
                    sum(row["fitted_zipf_r2_mean"] for row in fitted) / len(fitted)
                    if fitted else 0.0),
                "fit_scope": "forwarded_cache_misses",
            }
        return result


class Simulation(RealtimeSimulation):
    def __init__(self, config):
        super().__init__(config, "ReDNS")


def run_simulation(config):
    config = validate_config(config)
    if config["execution_mode"] == "realtime":
        return Simulation(config).run()
    return DeterministicSimulation(config).run()


def main(argv=None):
    return run_experiment(run_simulation, "ReDNS", "ReDNS_results.json", argv)


if __name__ == "__main__":
    raise SystemExit(main())
