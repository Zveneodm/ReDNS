from copy import deepcopy
from hashlib import sha256
import json
from struct import pack
import unittest
from unittest.mock import patch

from lib.analysis.origin import analyze_saved_result, analyze_trace, authority_threshold
from tests.helpers import config


class OriginAnalysisTests(unittest.TestCase):
    def fixture(self):
        cfg = config(num_domains=3, num_clients=4, num_forwarders=2,
                     warm_up_seconds=2, duration_seconds=8)
        ttls = [3, 100, 0]
        arrivals = [(0.1, 0, 0), (0.2, 1, 1),
                    (2.1, 2, 0), (2.2, 0, 2), (2.3, 1, 0),
                    (3.2, 0, 0), (3.3, 1, 1), (3.4, 1, 2),
                    (3.5, 0, 2), (4.0, 0, 1), (4.1, 1, 0), (5.4, 1, 0)]
        latencies = [1, 111, 111, 111, 0, 111, 111, 111, 111, 0]
        return cfg, ttls, arrivals, latencies

    def test_history_includes_warmup_and_references_are_separate(self):
        cfg, ttls, arrivals, latencies = self.fixture()
        result = analyze_trace(cfg, ttls, arrivals, latencies, threshold=111)
        self.assertEqual(result["warm_up_queries"], 2)
        self.assertEqual(result["total_queries"], 10)
        self.assertEqual(result["authoritative_queries"], 7)
        self.assertEqual(result["actual_origin_by_request_history"], {
            "first_global": 1, "first_cluster_seen_elsewhere": 3, "repeat_cluster": 3})
        self.assertEqual(result["all_window_unique_domains"], 3)
        self.assertEqual(result["all_window_unique_cluster_domain_pairs"], 6)
        self.assertEqual(result["actual_origin_by_ttl"]["0"]["origin_queries"], 3)
        refs = result["reference_replays"]
        self.assertEqual(refs["cluster"]["origin_queries"], 7)
        self.assertEqual(refs["cluster"]["first_cluster"], 4)
        self.assertEqual(refs["all_forwarders"]["origin_queries"], 4)
        # A reference hit need not prove a historical directory failure; real
        # refresh times change later TTL boundaries in both directions.
        self.assertEqual(refs["actual_origin_against_cluster_reference"]["cache_hit"], 1)
        self.assertEqual(refs["actual_cache_against_cluster_reference"]["ttl_expired"], 1)
        self.assertFalse(result["reference_assumptions"]["strict_lower_bound"])

    def test_rtt_classifier_rejects_ambiguous_costs(self):
        cfg = config()
        result = {"config": cfg, "forwarder": {
            "peer_misses": 0, "cluster_misses": 1, "cluster_child_misses": 1}}
        self.assertEqual(authority_threshold(result), 111)
        result["forwarder"]["peer_misses"] = 1000
        with self.assertRaisesRegex(ValueError, "unambiguously"):
            authority_threshold(result)

    def test_authority_trace_distinguishes_waiters_with_same_origin_latency(self):
        cfg = config(num_domains=1, warm_up_seconds=0)
        arrivals = [(0.1, 0, 0), (0.1, 2, 0)]
        report = analyze_trace(cfg, [100], arrivals, [111, 111], threshold=None, origin_flags=[1, 0])
        self.assertEqual(report["total_queries"], 2)
        self.assertEqual(report["authoritative_queries"], 1)
        for flags in ([1], [1, 0, 0], [1, 2]):
            with self.subTest(flags=flags), self.assertRaisesRegex(ValueError, "authority trace"):
                analyze_trace(cfg, [100], arrivals, [111, 111], threshold=None, origin_flags=flags)
        with self.assertRaisesRegex(ValueError, "require an authority trace"):
            authority_threshold({"config": cfg, "forwarder": {"coalesced_queries": 1}})

    def test_incomplete_or_out_of_order_trace_is_rejected(self):
        cfg, ttls, arrivals, latencies = self.fixture()
        for bad in (latencies[:-1], latencies + [111]):
            with self.subTest(length=len(bad)), self.assertRaisesRegex(ValueError, "latency trace"):
                analyze_trace(cfg, ttls, arrivals, bad, threshold=111)
        with self.assertRaisesRegex(ValueError, "not ordered"):
            analyze_trace(cfg, ttls, list(reversed(arrivals)), latencies, threshold=111)

    def test_saved_fingerprints_and_counts_must_match(self):
        cfg, ttls, arrivals, latencies = self.fixture()
        fingerprint = sha256()
        for event in arrivals:
            fingerprint.update(pack("!dQQ", *event))
        saved = dict(experiment="ReDNS", config=cfg,
                     workload_fingerprint_sha256=fingerprint.hexdigest(),
                     ttl_fingerprint_sha256=sha256(json.dumps(ttls, separators=(",", ":")).encode()).hexdigest(),
                     request_latencies_ms=latencies, total_queries=10,
                     warm_up_queries=2, authoritative_queries=7,
                     client={"lru_evictions": 0, "random_evictions": 0},
                     forwarder={"peer_misses": 0, "cluster_misses": 0,
                                "cluster_child_misses": 0, "records_transferred": 0})
        with patch("lib.analysis.origin.authority_ttls", return_value=ttls), \
                patch("lib.analysis.origin.workload_arrivals", return_value=arrivals):
            self.assertTrue(analyze_saved_result(saved)["fingerprints_verified"])
            traced = deepcopy(saved)
            traced["request_authority_queries"] = [int(latency >= 111) for latency in latencies]
            traced["forwarder"]["coalesced_queries"] = 1
            report = analyze_saved_result(traced)
            self.assertTrue(report["fingerprints_verified"])
            self.assertEqual(report["authority_classification"], "request_trace")
            for field in ("workload_fingerprint_sha256", "ttl_fingerprint_sha256",
                          "total_queries", "warm_up_queries", "authoritative_queries"):
                bad = deepcopy(saved)
                bad[field] = "wrong"
                with self.subTest(field=field), self.assertRaises(ValueError):
                    analyze_saved_result(bad)
