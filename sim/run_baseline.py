"""Run the baseline DNS experiment and save its statistics in res/."""

from bisect import bisect_right
from copy import deepcopy
from math import fsum
from pathlib import Path
import random
import sys

if __package__ in (None, ""):
    # Support `python sim/run_baseline.py` from any working directory.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.experiment import run_experiment, validate_config
from sim.clock import SimulationClock
from sim.latency import LatencyTracker
from sim.realtime import RealtimeSimulation
from sim.progress import Progress
from sim.preferences import HierarchicalZipf
from src.baseline.Model import AuthoritativeServer, Client, Forwarder, PublicDNSServer
from src.baseline.Worker import Worker


class DeterministicSimulation:
    """Original tick-based reference runner for regression comparisons."""

    experiment_name = "baseline"
    public_server_class = PublicDNSServer
    forwarder_class = Forwarder
    client_class = Client

    def __init__(self, config):
        self.config = validate_config(config)
        cfg = self.config
        self.traffic_rng = random.Random(cfg["seed"])
        ttl_rng = random.Random(cfg["seed"] + 1)
        #setup the authoritative server
        self.domains = [f"domain-{i:05d}.example" for i in range(cfg["num_domains"])]
        distribution = cfg["ttl_distribution"]
        if distribution is not None:
            # Build cumulative weights once for all domains; keep TTL sampling
            # independent of the query stream for comparable experiments.
            ttls = ttl_rng.choices(
                distribution["values"], weights=distribution["probabilities"],
                k=len(self.domains),
            )
        else:
            ttls = (ttl_rng.randint(*cfg["ttl_range"]) for _ in self.domains)
        self.authority = AuthoritativeServer(dict(zip(self.domains, ttls)))
        #setup the public DNS server, forwarders, and clients
        self.public_server = self.public_server_class(cfg["pub_cache_size"], self.authority)
        self.forwarders = [
            self.forwarder_class(cfg["forwarder_cache_size"], self.public_server, i)
            for i in range(cfg["num_forwarders"])
        ]
        self.clients = [
            self.client_class(cfg["client_cache_size"], self.forwarders[i % len(self.forwarders)], i)
            for i in range(cfg["num_clients"])
        ]
        self.workers = [Worker(client) for client in self.clients]
        self.resolvers = [*self.clients, *self.forwarders, self.public_server]
        self.latency_tracker = LatencyTracker()
        self.clock = SimulationClock()
        for resolvers, rtt in (
            (self.clients, cfg["client_forwarder_RTT"]),
            (self.forwarders, cfg["forwarder_pub_RTT"]),
            ([self.public_server], cfg["pub_auth_RTT"]),
        ):
            for resolver in resolvers:
                resolver.latency_tracker = self.latency_tracker
                resolver.clock = self.clock
                resolver.upstream_rtt_ms = rtt
        self.request_latencies_ms = []
        self.elapsed_seconds = 0
        self.warm_up_elapsed_seconds = 0
        self._warm_up_complete = False
        self.total_queries = 0
        self.hierarchical = (HierarchicalZipf(cfg)
                             if cfg["regional_preferences"]["enabled"] else None)
        if self.hierarchical is not None:
            self.hierarchical.prepare(cfg["num_clients"])
        self._active_client_id = 0
        self.cumulative_weights = []
        if cfg["query_distribution"] == "zipf":
            total = 0.0
            for rank in range(1, len(self.domains) + 1):
                total += rank ** -cfg["zipf_alpha"]
                self.cumulative_weights.append(total)

    def _choose_domain(self):
        if self.hierarchical is not None:
            index = self.hierarchical.sample(self._active_client_id, self.traffic_rng)
            return self.domains[index]
        if not self.cumulative_weights:
            return self.traffic_rng.choice(self.domains)
        position = self.traffic_rng.random() * self.cumulative_weights[-1]
        index = min(bisect_right(self.cumulative_weights, position), len(self.domains) - 1)
        return self.domains[index]

    def _run_second(self, *, measure):
        # All queries in a second occur before the next clock tick.
        for _ in range(self.config["queries_per_second"]):
            worker = self.traffic_rng.choice(self.workers)
            self._active_client_id = worker.client.client_id
            self.latency_tracker.reset()
            worker.send_query(self._choose_domain())
            if measure:
                self.request_latencies_ms.append(self.latency_tracker.latency_ms)
                self.total_queries += 1
        # Cached TTLs derive from deadlines; advance time once without visiting
        # caches. The authoritative records and returned copies have no clock.
        self.clock.advance()

    def run(self):
        progress = Progress(self.config["warm_up_seconds"] + self.config["duration_seconds"])
        progress.update(self.warm_up_elapsed_seconds + self.elapsed_seconds)
        try:
            result = self._run(progress)
            progress.finish()
            return result
        finally:
            progress.close()

    def _run(self, progress):
        # Retain warm caches and the advanced query stream, but measure only
        # the configured duration after warm-up.
        if not self._warm_up_complete:
            while self.warm_up_elapsed_seconds < self.config["warm_up_seconds"]:
                self._run_second(measure=False)
                self.warm_up_elapsed_seconds += 1
                progress.update(self.warm_up_elapsed_seconds)
            for resolver in self.resolvers:
                for key in resolver.stats:
                    resolver.stats[key] = 0
            self.authority.queries = 0
            self._warm_up_complete = True
        # Advance simulated time without sleeping.
        while self.elapsed_seconds < self.config["duration_seconds"]:
            self._run_second(measure=True)
            self.elapsed_seconds += 1
            progress.update(self.warm_up_elapsed_seconds + self.elapsed_seconds)
        return self.results()

    @staticmethod
    def _aggregate(resolvers):
        nodes = [resolver.metrics() for resolver in resolvers]
        result = {
            key: sum(node[key] for node in nodes)
            for key in nodes[0]
            if key not in ("hit_rate", "transfer_use_rate", "transfer_waste_rate")
        }
        result["hit_rate"] = result["hits"] / result["queries"] if result["queries"] else 0.0
        if "transfers_accepted" in result:
            accepted = result["transfers_accepted"]
            result["transfer_use_rate"] = (
                result["transfer_records_used"] / accepted if accepted else 0.0)
            unused = sum(result[f"transfer_unused_{reason}"]
                         for reason in ("expired", "evicted", "replaced"))
            result["transfer_waste_rate"] = unused / accepted if accepted else 0.0
        return result

    def results(self):
        total_latency_ms = fsum(self.request_latencies_ms)
        return {
            "experiment": self.experiment_name,
            "config": deepcopy(self.config),
            "elapsed_seconds": self.elapsed_seconds,
            "warm_up_elapsed_seconds": self.warm_up_elapsed_seconds,
            "total_queries": self.total_queries,
            "latency_unit": "ms",
            "request_latencies_ms": list(self.request_latencies_ms),
            "total_latency_ms": total_latency_ms,
            "average_latency_ms": total_latency_ms / self.total_queries if self.total_queries else 0.0,
            "client": self._aggregate(self.clients),
            "forwarder": self._aggregate(self.forwarders),
            "public_dns": self.public_server.metrics(),
            "authoritative_queries": self.authority.queries,
            "clients": [
                {"client_id": c.client_id, "forwarder_id": c.forwarder.forwarder_id, **c.metrics()}
                for c in self.clients
            ],
            "forwarders": [
                {"forwarder_id": f.forwarder_id, **f.metrics()} for f in self.forwarders
            ],
        }


class Simulation(RealtimeSimulation):
    def __init__(self, config):
        super().__init__(config, "baseline")


def run_simulation(config):
    config = validate_config(config)
    if config["execution_mode"] == "realtime":
        return Simulation(config).run()
    return DeterministicSimulation(config).run()


def main(argv=None):
    return run_experiment(run_simulation, "Baseline", "baseline_results.json", argv)


if __name__ == "__main__":
    raise SystemExit(main())
