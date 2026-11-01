"""Transport-independent query, directory and transfer protocol.

The runtime supplies a clock and implements _send(). No process primitives,
workload generators or wall-time scheduling belong in this module.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import partial
from typing import Callable, Optional

from src.cache import new_stats
from src.cluster import ForwarderClusters
from src.manager import ClusterManager, new_manager_stats
from src.models import AuthoritativeServer, BaselineForwarder, Client, ReDNSForwarder
from src.records import Record
from src.replacement import replacement_seed
from src.auxiliary.prefetch_protocol import PrefetchProtocol
from src.variants import COOPERATIVE_MODELS, MODELS


@dataclass
class _QueryWaiter:
    done: Callable
    measure: bool
    requester: Optional[int]
    completed_rtt: float = 0.0


@dataclass
class _InflightQuery:
    leader: _QueryWaiter
    waiters: list = field(default_factory=list)
    completed_rtt: float = 0.0


class DNSProtocol(PrefetchProtocol, ABC):
    def __init__(self, role, model, config, clock, *, ttls=(), node_ids=None, endpoint=None,
                 manager_shards=1, manager_shard=0):
        if role not in ("client", "forwarder", "authority", "manager"):
            raise ValueError(f"unknown role: {role}")
        if model not in MODELS:
            raise ValueError(f"unknown model: {model}")
        self.role, self.model, self.cfg, self.clock = role, model, config, clock
        self.cooperation_enabled = model in COOPERATIVE_MODELS
        self.endpoint = endpoint
        self.clusters = ForwarderClusters(config["num_forwarders"], config["forwarder_cluster_size"])
        self.nodes = []
        self.node_ids = ()
        self.formal_stats = []
        self.warm_stats = {}
        self.pending = {}
        # Transport tokens live in pending; foreground resolutions are indexed
        # separately by the owning forwarder and its actual cache key.
        self.inflight_queries = {}
        self.token = 0
        self.authority = None
        self.managers = {}
        self.manager_formal_stats = {}
        self.manager_warm_stats = {}
        self.region_announced = {}
        self.region_version = 0
        if role == "manager":
            if model == "baseline":
                raise ValueError("baseline does not have cluster managers")
            for first in range(0, config["num_forwarders"], self.clusters.size):
                cluster_id = first // self.clusters.size
                if node_ids is not None and cluster_id not in node_ids:
                    continue
                manager = ClusterManager(cluster_id, self.clusters.members(first), clock)
                self.managers[cluster_id] = manager
                self.manager_formal_stats[cluster_id] = manager.stats
        elif role == "authority":
            self.authority = AuthoritativeServer(ttls, self.clock)
        else:
            count = config["num_clients"] if role == "client" else config["num_forwarders"]
            size = config["client_cache_size"] if role == "client" else config["forwarder_cache_size"]
            node_class = (Client if role == "client" else
                          ReDNSForwarder if self.cooperation_enabled else BaselineForwarder)
            self.node_ids = range(count) if node_ids is None else node_ids
            self.nodes = [None] * count
            self.formal_stats = [None] * count
            for index in self.node_ids:
                node = node_class(size, self.clock,
                                  replacement_policy=config["replacement_policy"],
                                  replacement_seed=replacement_seed(config["seed"], role, index))
                node._runtime_measure = False
                if self.cooperation_enabled:
                    if role == "client":
                        node.on_store = partial(self._cache_stored, index)
                        node.on_remove = partial(self._cache_removed, index)
                    else:
                        node.on_evict = partial(self._transfer, index)
                        node.on_store = partial(self._region_stored, index)
                        node.on_remove = partial(self._region_removed, index)
                self.nodes[index] = node
                self.formal_stats[index] = node.stats
        self._init_prefetch(manager_shards, manager_shard)

    @abstractmethod
    def _send(self, role, message):
        """Queue a message for transport to another owner."""

    def _cache_stored(self, client_id, record):
        node = self.nodes[client_id]
        node.stats["directory_stores_sent"] += 1
        self._send("forwarder", ("cache_stored", client_id % self.cfg["num_forwarders"],
                                client_id, record.domain, record.expires_at, node._runtime_measure))

    def _cache_removed(self, client_id, record, reason):
        node = self.nodes[client_id]
        node.stats["directory_removes_sent"] += 1
        self._send("forwarder", ("cache_removed", client_id % self.cfg["num_forwarders"],
                                client_id, record.domain, record.expires_at, node._runtime_measure))

    def _region_stored(self, node_id, record):
        self._advertise_region(node_id, record.domain)

    def _region_removed(self, node_id, record, reason):
        self._advertise_region(node_id, record.domain)

    def _advertise_region(self, node_id, domain, *, force=False):
        """Publish one merged availability value, never individual copy types."""
        if len(self.clusters.members(node_id)) <= 1:
            return
        node = self.nodes[node_id]
        deadline = node.region_deadline(domain)
        key = (node_id, domain)
        if not force and self.region_announced.get(key, 0) == deadline:
            return
        if deadline:
            self.region_announced[key] = deadline
        else:
            self.region_announced.pop(key, None)
        self.region_version += 1
        node.stats["manager_updates_sent"] += 1
        self._send("manager", ("region_update", node_id // self.clusters.size, node_id,
                               domain, deadline, self.region_version, node._runtime_measure))

    def _select_manager(self, cluster_id, measure):
        if self.role != "manager" or self.model == "baseline":
            raise ValueError("directory messages must be handled by cluster managers")
        manager = self.managers[cluster_id]
        if measure:
            manager.stats = self.manager_formal_stats[cluster_id]
        else:
            stats = self.manager_warm_stats.get(cluster_id)
            if stats is None:
                stats = self.manager_warm_stats[cluster_id] = new_manager_stats()
            manager.stats = stats
        return manager

    def _select(self, node_id, measure):
        node = self.nodes[node_id]
        if node is None:
            raise ValueError(f"node {node_id} is owned by another worker")
        if measure:
            node.stats = self.formal_stats[node_id]
        else:
            stats = self.warm_stats.get(node_id)
            if stats is None:
                stats = new_stats()
                self.warm_stats[node_id] = stats
            node.stats = stats
        node._runtime_measure = measure
        return node

    def _upstream(self, node_id):
        if self.role == "client":
            return "forwarder", node_id % self.cfg["num_forwarders"], self.cfg["client_forwarder_RTT"]
        return "authority", 0, self.cfg["forwarder_authoritative_RTT"]

    def _new_token(self):
        self.token += 1
        return (self.endpoint, self.token) if self.endpoint is not None else self.token

    def _fetch(self, node_id, domain, fill, measure, done, *, trace=None):
        role, target, _ = self._upstream(node_id)
        token = self._new_token()
        def received(answer, authority_query):
            if trace is not None:
                trace["authority_query"] = authority_query
            done(answer)

        self.pending[token] = received
        requester = node_id if self.role == "client" else None
        self._send(role, ("query", self.endpoint or self.role, token, target, domain, fill, measure, requester))

    def _query(self, node_id, domain, fill, measure, done, *, requester=None, trace=None):
        trace = {} if trace is None else trace
        trace["authority_query"] = False
        done = self._demand_started(node_id, domain, done, measure=measure)
        if self.role == "authority":
            trace["authority_query"] = True
            record = self.authority.lookup(domain, measure=measure)
            done((record.expires_at, record.auth_ttl, 0.0))
            return
        node = self._select(node_id, measure)
        node.stats["queries"] += 1
        record = node.check_cache(domain, foreground=fill)
        if record is not None:
            node.stats["hits"] += 1
            done((record.expires_at, record.auth_ttl, 0.0))
            return
        node.stats["misses"] += 1
        if self.role == "forwarder" and fill:
            self._forwarder_miss(node_id, domain, measure, done, requester, trace)
            return
        _, _, rtt = self._upstream(node_id)

        def received(answer, extra_latency=0.0):
            expiry, auth_ttl, latency = answer
            current = self._select(node_id, measure)
            if fill:
                current.insert_cache(Record(domain, expiry, auth_ttl))
            done((expiry, auth_ttl, latency + (rtt if fill else 0.0) + extra_latency))

        self._select(node_id, measure).stats["upstream_queries"] += 1
        self._fetch(node_id, domain, fill, measure, received, trace=trace)

    def _forwarder_miss(self, node_id, domain, measure, done, requester, trace):
        key = (node_id, domain)
        flight = self.inflight_queries.get(key)
        if flight is not None:
            self._select(node_id, measure).stats["coalesced_queries"] += 1
            flight.waiters.append(_QueryWaiter(done, measure, requester, flight.completed_rtt))
            return
        flight = _InflightQuery(_QueryWaiter(done, measure, requester))
        self.inflight_queries[key] = flight

        def progress(completed_rtt):
            flight.completed_rtt = completed_rtt

        def completed(answer, source_kind, source_client=None):
            # Release before cache callbacks and replies. This also removes the
            # entry if a cache callback raises, and permits reentrant queries.
            self.inflight_queries.pop(key)
            expiry, auth_ttl, full_rtt = answer
            self._select(node_id, measure).insert_cache(Record(domain, expiry, auth_ttl))
            for waiter, joined in [(flight.leader, False)] + [(item, True) for item in flight.waiters]:
                current = self._select(node_id, waiter.measure)
                remaining_rtt = full_rtt
                if joined:
                    # The simulator accumulates completed link RTTs rather than
                    # sleeping for network transit. Charge only links still
                    # unresolved at join, never IPC wall time or a new fetch.
                    remaining_rtt = max(0.0, full_rtt - waiter.completed_rtt)
                    current.stats["coalesced_wait_latency_ms"] += remaining_rtt
                    current.stats[f"coalesced_{source_kind}_waits"] += 1
                if (source_client is not None and waiter.requester is not None
                        and source_client != waiter.requester):
                    current.stats["successful_reverse_queries"] += 1
                waiter.done((expiry, auth_ttl, remaining_rtt))

        try:
            self._resolve_forwarder_miss(node_id, domain, measure, completed, progress, trace)
        except BaseException:
            self.inflight_queries.pop(key, None)
            raise

    def _resolve_forwarder_miss(self, node_id, domain, measure, done, progress, trace):
        def upstream(extra_latency=0.0):
            self._select(node_id, measure).stats["upstream_queries"] += 1

            def received(answer):
                expiry, auth_ttl, latency = answer
                done((expiry, auth_ttl, latency + self.cfg["forwarder_authoritative_RTT"]
                      + extra_latency), "authority")

            self._fetch(node_id, domain, True, measure, received, trace=trace)

        if isinstance(self.nodes[node_id], ReDNSForwarder):
            def local_received(answer, local_cost):
                if answer is not None:
                    expiry, auth_ttl, source_client, _ = answer
                    done((expiry, auth_ttl, local_cost), "peer", source_client)
                    return

                def cluster_received(cluster_answer, cluster_cost):
                    if cluster_answer is None:
                        upstream(local_cost + cluster_cost)
                        return
                    expiry, auth_ttl, source_client, _ = cluster_answer
                    kind = "cluster_forwarder" if source_client is None else "cluster_client"
                    done((expiry, auth_ttl, local_cost + cluster_cost), kind, source_client)

                self._cluster_lookup(node_id, domain, measure, cluster_received,
                                     progress=lambda cost: progress(local_cost + cost))

            self._probe_clients(node_id, domain, measure, local_received, progress=progress)
            return
        upstream()

    def _probe_clients(self, node_id, domain, measure, done, *, assisting=False, progress=None):
        """Try only directory-advertised direct clients; never recurse upstream.

        Every actual probe costs one RTT, including stale hints. A failed hint
        is removed and remaining candidates are tried once each.
        """
        prefix = "cluster_child_" if assisting else "peer_"
        attempted = set()
        elapsed = 0.0

        def attempt():
            nonlocal elapsed
            node = self._select(node_id, measure)
            target = node.peer_target(domain, exclude=attempted)
            if target is None:
                self._advertise_region(node_id, domain)
                done(None, elapsed)
                return
            attempted.add(target)
            known_expiry = node.copy_directory.deadline(target, domain)
            node.stats[prefix + "queries"] += 1

            def received(answer):
                nonlocal elapsed
                elapsed += self.cfg["client_forwarder_RTT"]
                if progress is not None:
                    progress(elapsed)
                current = self._select(node_id, measure)
                if answer is None:
                    current.stats[prefix + "misses"] += 1
                    current.copy_directory.remove(target, domain, known_expiry)
                    self._advertise_region(node_id, domain)
                    attempt()
                else:
                    expiry, auth_ttl, staged = answer
                    current.stats[prefix + "hits"] += 1
                    if not assisting:
                        current.stats["peer_transfer_hits"] += int(staged)
                    done((expiry, auth_ttl, target, staged), elapsed)

            token = self._new_token()
            self.pending[token] = received
            self._send("client", ("peer_query", token, target, domain, measure))

        attempt()

    def _cluster_lookup(self, node_id, domain, measure, done, *, progress=None):
        """Ask the manager once, then contact only its live region hints."""
        if len(self.clusters.members(node_id)) <= 1:
            done(None, 0.0)
            return
        node = self._select(node_id, measure)
        node.stats["manager_queries"] += 1

        def located(candidates):
            current = self._select(node_id, measure)
            current.stats["manager_directory_hits" if candidates else "manager_directory_misses"] += 1
            remaining = iter(candidates)
            elapsed = self.cfg["forwarder_manager_RTT"]
            if progress is not None:
                progress(elapsed)

            def attempt():
                nonlocal elapsed
                candidate = next(remaining, None)
                while candidate is not None and candidate[1] <= self.clock.now:
                    candidate = next(remaining, None)
                if candidate is None:
                    done(None, elapsed)
                    return
                target, _, version = candidate
                if not self.clusters.can_query(node_id, target):
                    raise ValueError("manager returned a region outside the requester's cluster")
                self._select(node_id, measure).stats["cluster_queries"] += 1

                def received(answer, helper_cost):
                    nonlocal elapsed
                    elapsed += self.cfg["forwarder_forwarder_RTT"] + helper_cost
                    if progress is not None:
                        progress(elapsed)
                    origin = self._select(node_id, measure)
                    if answer is None:
                        origin.stats["cluster_misses"] += 1
                        origin.stats["manager_feedback_sent"] += 1
                        self._send("manager", ("manager_invalidate", node_id // self.clusters.size,
                                               node_id, target, domain, version, measure))
                        attempt()
                        return
                    _, _, source_client, staged = answer
                    origin.stats["cluster_hits"] += 1
                    kind = "cluster_forwarder_hits" if source_client is None else "cluster_client_hits"
                    origin.stats[kind] += 1
                    origin.stats["cluster_transfer_hits"] += int(staged)
                    done(answer, elapsed)

                token = self._new_token()
                self.pending[token] = received
                self._send("forwarder", ("cluster_query", node_id, token, target, domain, measure))

            attempt()

        token = self._new_token()
        self.pending[token] = located
        self._send("manager", ("manager_lookup", node_id // self.clusters.size,
                               node_id, token, domain, measure))

    def _serve_cluster(self, origin, token, node_id, domain, measure):
        """A helper checks itself and its children, without neighbors/authority."""
        if (self.role != "forwarder" or not self.cooperation_enabled
                or not self.clusters.can_query(origin, node_id)):
            raise ValueError("cluster queries must stay between distinct ReDNS forwarders in one group")
        node = self._select(node_id, measure)
        node.stats["cluster_queries_received"] += 1
        if self.prefetch_enabled:
            self.prefetch_load[node_id] += 1

        def reply(answer, helper_cost):
            current = self._select(node_id, measure)
            if answer is None:
                self._advertise_region(node_id, domain, force=True)
                current.stats["cluster_misses_served"] += 1
            else:
                current.stats["cluster_hits_served"] += 1
            self._send("forwarder", ("cluster_answer", token, answer, helper_cost))

        record = node.check_cache(domain)
        if record is not None:
            node.stats["cluster_cache_hits_served"] += 1
            reply((record.expires_at, record.auth_ttl, None, False), 0.0)
            return

        def child_received(answer, helper_cost):
            if answer is not None:
                expiry, auth_ttl, _, _ = answer
                self._select(node_id, measure).insert_cache(Record(domain, expiry, auth_ttl))
            reply(answer, helper_cost)

        self._probe_clients(node_id, domain, measure, child_received, assisting=True)

    def _transfer(self, node_id, record):
        node = self.nodes[node_id]
        clients = range(node_id, self.cfg["num_clients"], self.cfg["num_forwarders"])
        target = node.transfer_target(record, clients, self.cfg["client_cache_size"])
        if target is not None:
            node.stats["records_transferred"] += 1
            self._send("client", ("store", target, record.domain, record.expires_at,
                                  record.auth_ttl, node._runtime_measure))

    def handle_message(self, message):
        if message[0].startswith("prefetch_"):
            if not self.prefetch_enabled or not self._prefetch_message(message):
                raise ValueError("unexpected prefetch message")
            return
        kind = message[0]
        if kind == "query":
            _, sender, token, node, domain, fill, measure, requester = message
            trace = {}
            self._query(node, domain, fill, measure,
                        lambda answer: self._send(sender, ("query_answer", token, answer,
                                                          trace["authority_query"])),
                        requester=requester, trace=trace)
        elif kind == "query_answer":
            _, token, answer, authority_query = message
            self.pending.pop(token)(answer, authority_query)
        elif kind in ("answer", "peer_answer", "manager_answer"):
            _, token, answer = message
            self.pending.pop(token)(answer)
        elif kind == "region_update":
            _, cluster_id, forwarder, domain, deadline, version, measure = message
            self._select_manager(cluster_id, measure).update(forwarder, domain, deadline, version)
        elif kind == "manager_lookup":
            _, cluster_id, requester, token, domain, measure = message
            candidates = self._select_manager(cluster_id, measure).lookup(requester, domain)
            self._send("forwarder", ("manager_answer", token, candidates))
        elif kind == "manager_invalidate":
            _, cluster_id, reporter, forwarder, domain, version, measure = message
            self._select_manager(cluster_id, measure).invalidate(reporter, forwarder, domain, version)
        elif kind == "cluster_query":
            _, origin, token, node_id, domain, measure = message
            self._serve_cluster(origin, token, node_id, domain, measure)
        elif kind == "cluster_answer":
            _, token, answer, helper_cost = message
            self.pending.pop(token)(answer, helper_cost)
        elif kind == "peer_query":
            _, token, node_id, domain, measure = message
            node = self._select(node_id, measure)
            node.stats["peer_queries_received"] += 1
            entry = node.cache.get(domain)
            staged = entry is not None and entry.transfer_stats is not None
            record = node.check_cache(domain)
            answer = None
            if record is not None:
                node.stats["peer_hits_served"] += 1
                answer = (record.expires_at, record.auth_ttl, staged)
            self._send("forwarder", ("peer_answer", token, answer))
        elif kind == "store":
            _, node_id, domain, expiry, auth_ttl, measure = message
            self._select(node_id, measure).receive_transfer(Record(domain, expiry, auth_ttl))
        elif kind in ("cache_removed", "cache_stored"):
            _, parent_node, child_id, domain, expiry, measure = message
            node = self._select(parent_node, measure)
            if not isinstance(node, ReDNSForwarder):
                raise RuntimeError("baseline received a cache notification")
            if child_id % self.cfg["num_forwarders"] != parent_node:
                raise RuntimeError("cache notification routed to the wrong forwarder")
            if kind == "cache_removed":
                node.stats["directory_removes_received"] += 1
                node.copy_directory.remove(child_id, domain, expiry)
            else:
                node.stats["directory_stores_received"] += 1
                node.copy_directory.store(child_id, domain, expiry)
            self._advertise_region(parent_node, domain)
        else:
            raise ValueError(f"unknown message: {kind}")
