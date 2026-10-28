"""Seeded global Poisson arrivals and independent client/domain choices."""

from array import array
from bisect import bisect_right
import math
import random

from sim.preferences import HierarchicalZipf


class Workload:
    def __init__(self, config):
        self.config = config
        self.arrival_rng = random.Random(config["seed"] + 2)
        self.query_rng = random.Random(config["seed"])
        self.hierarchical = (HierarchicalZipf(config)
                             if config["regional_preferences"]["enabled"] else None)
        if self.hierarchical is not None:
            self.hierarchical.prepare(config["num_clients"])
        self.weights = array("d")
        if config["query_distribution"] == "zipf" and self.hierarchical is None:
            total = 0.0
            for rank in range(1, config["num_domains"] + 1):
                total += rank ** -config["zipf_alpha"]
                self.weights.append(total)
        self.next_time = self.interval()

    def interval(self):
        rate = self.config["queries_per_second"]
        return self.arrival_rng.expovariate(rate) if rate else math.inf

    def pop(self):
        when = self.next_time
        client = self.query_rng.randrange(self.config["num_clients"])
        if self.hierarchical is not None:
            domain = self.hierarchical.sample(client, self.query_rng)
        elif self.weights:
            position = self.query_rng.random() * self.weights[-1]
            domain = min(bisect_right(self.weights, position), len(self.weights) - 1)
        else:
            domain = self.query_rng.randrange(self.config["num_domains"])
        self.next_time += self.interval()
        return when, client, domain
