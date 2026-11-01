from contextlib import redirect_stdout
from copy import deepcopy
import io
from pathlib import Path
import tempfile
import unittest

from sim.analyze_origin import analyze_saved_result
from sim.clock import ManualClock
from sim.comparison import validate_pair
from sim.config import validate_config
from sim.metrics import aggregate, validate_results
from sim.parallel import WorkerLayout
from sim.realtime import RealtimeSimulation, _Layer
from src.records import Record
from tests.helpers import ProtocolHarness, config


class ShardedHarness:
    def __init__(self, model="ReDNS", **overrides):
        self.config = config(**overrides)
        self.clock = ManualClock()
        self.layout = WorkerLayout(self.config, model)
        self.layers = {}
        for name, spec in self.layout.workers.items():
            layer = _Layer(spec["role"], model, self.config, {}, None, None,
                           layout=self.layout, endpoint=name)
            layer.clock = self.clock
            for node_id in layer.node_ids:
                layer.nodes[node_id].clock = self.clock
            if layer.authority is not None:
                layer.authority.clock = self.clock
            for manager in layer.managers.values():
                manager.clock = self.clock
            self.layers[name] = layer

    def owner(self, role, node):
        return self.layers[self.layout.owner(role, node)]

    def drain(self):
        for _ in range(10000):
            worked = False
            for layer in self.layers.values():
                while layer.outbox:
                    name, message = layer.outbox.popleft()
                    self.layers[name]._message(message)
                    worked = True
            if not worked:
                return
        raise AssertionError("sharded protocol did not drain")

    def query(self, client, domain):
        replies = []
        self.owner("client", client)._query(client, domain, True, True, replies.append)
        self.drain()
        return replies[0]


class ParallelTests(unittest.TestCase):
    def test_worker_counts_are_positive_and_copied(self):
        for workers in (None, {"authority": 2}, {"client": 0}, {"forwarder": True},
                        {"manager": 1.5}, {"client": -1}):
            with self.subTest(workers=workers), self.assertRaises(ValueError):
                validate_config({"workers": workers})
        cfg = validate_config({"workers": {"client": 3}})
        cfg["workers"]["manager"] = 20
        self.assertEqual(validate_config({})["workers"], {"client": 1, "forwarder": 1, "manager": 1})

    def test_ownership_is_disjoint_and_baseline_has_same_common_layout(self):
        cfg = config(num_clients=31, num_forwarders=10, forwarder_cluster_size=3,
                     workers={"client": 4, "forwarder": 8, "manager": 2})
        redns, baseline = WorkerLayout(cfg, "ReDNS"), WorkerLayout(cfg, "baseline")
        self.assertEqual(len(redns.workers), 16)
        self.assertEqual(len(baseline.workers), 14)
        for name, spec in baseline.workers.items():
            self.assertEqual(spec, redns.workers[name])
        for role, total in (("client", 31), ("forwarder", 10)):
            ids = [node for spec in redns.workers.values() if spec["role"] == role
                   for node in spec["node_ids"]]
            self.assertEqual(sorted(ids), list(range(total)))
            for node in ids:
                self.assertIn(node, redns.workers[redns.owner(role, node)]["node_ids"])

    def test_one_cluster_manager_can_use_multiple_domain_shards(self):
        cfg = config(num_forwarders=10, forwarder_cluster_size=100, workers={"manager": 2})
        layout = WorkerLayout(cfg, "ReDNS")
        self.assertEqual({layout.owner("manager", 0, d) for d in range(20)}, {"manager:0", "manager:1"})
        for message in (("region_update", 0, 1, 5, 100, 1, True),
                        ("manager_lookup", 0, 1, ("forwarder", 1), 5, True),
                        ("manager_invalidate", 0, 1, 2, 5, 1, True)):
            self.assertEqual(layout.destination("manager", message), "manager:1")

    def test_concurrent_cross_shard_queries_do_not_collide_tokens(self):
        h = ShardedHarness(num_clients=16, num_forwarders=4, forwarder_cluster_size=2,
                           client_cache_size=4, forwarder_cache_size=0,
                           workers={"client": 3, "forwarder": 4, "manager": 2})
        for client, domain in ((1, 7), (3, 8)):
            expiry = 100 if domain == 7 else 200
            h.owner("client", client)._select(client, True).insert_cache(Record(domain, expiry, expiry))
        h.drain()
        replies = {0: [], 2: []}
        for client, domain in ((0, 7), (2, 8)):
            h.owner("client", client)._query(client, domain, True, True, replies[client].append)
        h.drain()
        self.assertEqual(replies[0], [(100, 100, 5)])
        self.assertEqual(replies[2], [(200, 200, 5)])
        for forwarder in (0, 2):
            node = h.owner("forwarder", forwarder).nodes[forwarder]
            self.assertEqual(node.stats["successful_reverse_queries"], 1)
        self.assertEqual(h.layers["authority"].authority.queries, 0)
        self.assertTrue(all(not layer.pending and not layer.outbox for layer in h.layers.values()))

    def test_sharding_preserves_sequential_protocol_answers_and_counters(self):
        options = dict(num_clients=12, num_forwarders=4, forwarder_cluster_size=2,
                       num_domains=20, client_cache_size=2, forwarder_cache_size=2,
                       ttl_range=[3, 3])
        for model in ("baseline", "ReDNS"):
            ordinary = ProtocolHarness(model, **options)
            sharded = ShardedHarness(model, workers={"client": 3, "forwarder": 4, "manager": 2}, **options)
            for step in range(160):
                when = step * 0.25
                ordinary.clock.advance_to(when)
                sharded.clock.advance_to(when)
                client, domain = step % 12, (step * 7 + step // 12) % 20
                self.assertEqual(ordinary.query(client, domain), sharded.query(client, domain))
            for role in ("client", "forwarder"):
                expected = aggregate([node.metrics() for node in ordinary.layers[role].nodes])
                actual = aggregate([layer.nodes[node].metrics() for name, layer in sharded.layers.items()
                                    if sharded.layout.workers[name]["role"] == role for node in layer.node_ids])
                self.assertEqual(expected, actual)

    def test_real_sharded_no_cache_preserves_trace_cpu_and_rtt(self):
        cfg = config(num_clients=12, num_forwarders=6, forwarder_cluster_size=3,
                     workers={"client": 2, "forwarder": 3, "manager": 2},
                     warm_up_seconds=1, duration_seconds=1, queries_per_second=80,
                     client_cache_size=0, forwarder_cache_size=0, time_scale=0.25)
        rows = {}
        for model in ("baseline", "ReDNS"):
            simulation = RealtimeSimulation(cfg, model)
            with redirect_stdout(io.StringIO()):
                result = simulation.run()
            self.assertTrue(all(not p.is_alive() for p in simulation.processes.values()))
            validate_results(result)
            self.assertEqual(len(result["workers"]), 9 if model == "ReDNS" else 7)
            self.assertEqual(result["authoritative_queries"], result["total_queries"])
            self.assertEqual(set(result["request_latencies_ms"]), {112 if model == "ReDNS" else 111})
            rows[model] = result
        validate_pair(rows, cfg, rows["baseline"]["cpu_pool"])
        self.assertTrue(analyze_saved_result(rows["ReDNS"])["fingerprints_verified"])
        bad = deepcopy(rows)
        bad["ReDNS"]["workers"]["client:0"]["cpu"] = -1
        with self.assertRaisesRegex(RuntimeError, "CPU binding"):
            validate_pair(bad, cfg, rows["baseline"]["cpu_pool"])

    def test_real_sharded_observation_and_cooperation_are_merged(self):
        cfg = config(num_clients=24, num_forwarders=6, forwarder_cluster_size=6,
                     workers={"client": 2, "forwarder": 3, "manager": 2},
                     warm_up_seconds=1, duration_seconds=2, queries_per_second=400,
                     num_domains=20, time_scale=0.25)
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            result = RealtimeSimulation(cfg, "ReDNS", observation={
                "output_dir": directory, "sample_clients": 24}).run()
            validate_results(result)
            self.assertGreater(result["cluster_hits"], 0)
            self.assertGreater(result["successful_reverse_queries"], 0)
            for role, count in (("client", 24), ("forwarder", 6)):
                profile = result[f"{role}_retention"]
                self.assertEqual(profile["node_count"], count)
                self.assertEqual(profile["totals"]["queries"], result[role]["queries"])
                self.assertTrue(Path(profile["path"]).exists())
