"""Per-forwarder Forestall prediction, with only quota shared at the manager."""

from collections import defaultdict
from dataclasses import dataclass
from heapq import heappop, heappush
from math import floor

from src.auxiliary.prefetch import Forestall


@dataclass(frozen=True)
class LocalGrant:
    observed_expiry: float
    valid_until: float
    issued_at: float
    measure: bool


class LocalPrefetchProtocol:
    def _init_local_prefetch(self):
        self.local_prefetchers = {
            node: Forestall(self.cfg["prefetch"]) for node in self.node_ids
        } if self.local_prefetch and self.role == "forwarder" else {}
        self.local_prefetch_events = defaultdict(list)
        self.local_prefetch_next_advice = defaultdict(lambda: self.cfg["prefetch"]["advice_seconds"])
        self.local_prefetch_grants = {}

    def _tick_local_prefetch(self):
        now = self.clock.now
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
        interval = self.cfg["prefetch"]["report_seconds"]
        if not self.prefetch_final and (now >= self.prefetch_epoch * interval or now >= end):
            final, epoch = now >= end, self.prefetch_epoch
            measure = now >= self.cfg["warm_up_seconds"]
            for node_id in self.node_ids:
                tracker = self.local_prefetchers[node_id]
                events = self.local_prefetch_events[node_id]
                for row in self.prefetch_observations.pop(node_id, ()):
                    heappush(events, (row[1], row[0], row[2], row[3]))
                watermark = min(self.prefetch_demand_pending[node_id].values(), default=now)
                node = self._select(node_id, measure)
                while events and events[0][0] < watermark:
                    arrival, domain, expiry, ttl = heappop(events)
                    tracker.observe(domain, arrival, expiry, ttl)
                    node.stats["prefetch_local_observations"] += 1
                advice = None
                if not final and now < end and now >= self.local_prefetch_next_advice[node_id]:
                    period = self.cfg["prefetch"]["advice_seconds"]
                    self.local_prefetch_next_advice[node_id] = (floor(now / period) + 1) * period
                    node.stats["prefetch_local_advice_runs"] += 1
                    selected = tracker.advise(now)
                    node.stats["prefetch_local_advised"] += len(selected)
                    advice = [(domain, deadline, now + period, tracker.tracking[domain].expires_at,
                               tracker.tracking[domain].intervals.mean)
                              for deadline, domain in selected]
                counts = tuple(self.prefetch_demand_counts.pop(node_id, [0, 0]))
                for phase, count in enumerate(counts):
                    self._select(node_id, bool(phase)).stats["prefetch_budget_demand_reported"] += count
                self._select(node_id, measure).stats["prefetch_reports_sent"] += 1
                # Raw demand samples stay local. Quota receives only demand
                # counts and pinned proposals produced by this predictor.
                self._send("manager", ("prefetch_local_report", node_id // self.clusters.size,
                                       node_id, epoch, counts, advice, measure))
                self.prefetch_load.pop(node_id, None)
            self.prefetch_epoch = epoch + 1
            self.prefetch_final = final
        self._drain_prefetch_queue(now)

    def _receive_local_prefetch_report(self, message):
        _, cluster, forwarder, epoch, counts, advice, measure = message
        if not self.local_prefetch or cluster not in self.prefetch_budgets:
            raise ValueError("local advice reached the wrong quota owner")
        manager = self._select_manager(cluster, measure)
        manager._check_member(forwarder)
        report_key = (cluster, forwarder)
        if epoch <= self.prefetch_report_epoch.get(report_key, 0):
            raise ValueError("duplicate or out-of-order local quota report")
        self.prefetch_report_epoch[report_key] = epoch
        manager.stats["prefetch_reports_received"] += 1
        budget = self.prefetch_budgets[cluster]
        changes = budget.fund(counts)
        for phase, count in enumerate(counts):
            stats = self._select_manager(cluster, bool(phase)).stats
            stats["prefetch_budget_demand"] += count
            stats["prefetch_budget_allowance"] += changes[phase]
        self._select_manager(cluster, measure)
        now = self.clock.now
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
        if advice is not None:
            advice = [row for row in advice
                      if max(now, row[1] - self.cfg["forwarder_authoritative_RTT"] / 1000) < end]
            budget.offer(forwarder, advice, now, target=forwarder)
        self._dispatch_prefetch_budget(cluster)
        if budget.pending and budget.available(measure) <= 0 and now < end:
            manager.stats["prefetch_budget_blocked"] += 1

    def _local_grant_is_stale(self, node_id, domain, grant):
        entry = self.local_prefetchers[node_id].tracking.get(domain)
        cached = self.nodes[node_id].cache.get(domain)
        return (entry is None or entry.expires_at > grant.observed_expiry or
                (cached is not None and cached.record.expires_at > grant.observed_expiry) or
                self.clock.now >= grant.valid_until or
                self.clock.now >= self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"])

    def _receive_local_prefetch_grant(self, message):
        _, node_id, domain, deadline, valid_until, known_expiry, issued_at, measure = message
        if not self.local_prefetch or self.role != "forwarder":
            raise ValueError("local grant must reach its baseline forwarder")
        node = self._select(node_id, measure)
        node.stats["prefetch_tasks_received"] += 1
        grant = LocalGrant(known_expiry, valid_until, issued_at, measure)
        key = (node_id, domain)
        if key in self.local_prefetch_grants:
            raise ValueError("duplicate local prefetch grant")
        self.local_prefetch_grants[key] = grant
        due = max(self.clock.now, deadline - self.cfg["forwarder_authoritative_RTT"] / 1000)
        if (self._local_grant_is_stale(node_id, domain, grant) or
                due >= self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]):
            node.stats["prefetch_skipped"] += 1
            self._finish_local_prefetch(node_id, domain, None)
            return
        heappush(self.prefetch_queue, (due, node_id, domain, measure))

    def _finish_local_prefetch(self, node_id, domain, expiry):
        grant = self.local_prefetch_grants.pop((node_id, domain))
        if expiry is not None:
            tracker = self.local_prefetchers[node_id]
            if domain in tracker.tracking:
                tracker.mark_advised(domain, grant.issued_at)
            tracker.refreshed(domain, expiry)
        self._send("manager", ("prefetch_local_result", node_id // self.clusters.size,
                               node_id, domain, expiry is not None, grant.measure))
