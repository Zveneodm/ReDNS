"""Batched cluster telemetry, manager advice and bounded background fetches."""

from collections import defaultdict
from heapq import heappop, heappush
from math import floor

from src.prefetch import Forestall
from src.records import Record


class PrefetchProtocol:
    def _init_prefetch(self, manager_shards, manager_shard):
        self.prefetch_enabled = self.model == "ReDNS-prefetch"
        self.manager_shards, self.manager_shard = manager_shards, manager_shard
        self.prefetch_epoch = 1
        self.prefetch_final = False
        self.prefetch_observations = defaultdict(list)
        self.prefetch_load = defaultdict(int)
        self.prefetch_active = defaultdict(int)
        self.prefetch_demand_pending = defaultdict(dict)
        self.prefetch_observation_token = 0
        self.prefetch_fetching = set()
        self.prefetch_queue = []
        self.prefetch_reports = {}
        self.prefetch_event_buffers = defaultdict(list)
        self.prefetchers = {cluster: Forestall(self.cfg["prefetch"]) for cluster in self.managers}
        self.prefetch_next_advice = defaultdict(lambda: self.cfg["prefetch"]["advice_seconds"])
        self.prefetch_inflight = {}

    def _demand_started(self, node_id, domain, done):
        if not self.prefetch_enabled or self.role != "forwarder":
            return done
        arrival = self.clock.now
        self.prefetch_observation_token += 1
        observation = self.prefetch_observation_token
        self.prefetch_demand_pending[node_id][observation] = arrival
        self.prefetch_load[node_id] += 1
        self.prefetch_active[node_id] += 1

        def completed(answer):
            self.prefetch_active[node_id] -= 1
            del self.prefetch_demand_pending[node_id][observation]
            expiry, ttl, _ = answer
            self.prefetch_observations[node_id].append((domain, arrival, expiry, ttl))
            done(answer)

        return completed

    def prefetch_tick(self):
        """Called by the runtime; bounded by the original arrival horizon."""
        if not self.prefetch_enabled or self.role != "forwarder":
            return
        now = self.clock.now
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
        interval = self.cfg["prefetch"]["report_seconds"]
        if not self.prefetch_final and (now >= self.prefetch_epoch * interval or now >= end):
            final = now >= end
            epoch = self.prefetch_epoch
            for node_id in self.node_ids:
                parts = [[] for _ in range(self.manager_shards)]
                cluster = node_id // self.clusters.size
                for row in self.prefetch_observations.pop(node_id, ()):
                    parts[(cluster * self.cfg["num_domains"] + row[0]) % self.manager_shards].append(row)
                measure = now >= self.cfg["warm_up_seconds"]
                node = self._select(node_id, measure)
                node.stats["prefetch_reports_sent"] += self.manager_shards
                node.stats["prefetch_observations_sent"] += sum(map(len, parts))
                load = self.prefetch_load.pop(node_id, 0) / interval + self.prefetch_active[node_id]
                # No later report can contain an arrival older than this bound:
                # unresolved demand replies keep the bound behind that arrival.
                watermark = min(self.prefetch_demand_pending[node_id].values(), default=now)
                for shard, rows in enumerate(parts):
                    self._send("manager", ("prefetch_report", cluster, node_id, shard,
                                           epoch, load, rows, watermark, final, measure))
            self.prefetch_epoch = epoch + 1
            self.prefetch_final = final
        for _ in range(128):
            if not self.prefetch_queue or self.prefetch_queue[0][0] > now:
                break
            _, node_id, domain, measure = heappop(self.prefetch_queue)
            self._execute_prefetch(node_id, domain, measure)

    def _receive_prefetch_report(self, message):
        _, cluster, forwarder, shard, epoch, load, rows, watermark, final, measure = message
        manager = self._select_manager(cluster, measure)
        manager._check_member(forwarder)
        if shard != self.manager_shard:
            raise ValueError("prefetch report reached the wrong domain shard")
        manager.stats["prefetch_reports_received"] += 1
        manager.stats["prefetch_observations_received"] += len(rows)
        manager.loads[forwarder] = load
        key = (cluster, epoch)
        reports = self.prefetch_reports.setdefault(key, {})
        if forwarder in reports:
            raise ValueError("duplicate prefetch report")
        reports[forwarder] = (rows, watermark)
        if len(reports) != len(manager.members):
            return
        events = self.prefetch_event_buffers[cluster]
        for batch, _ in reports.values():
            for domain, arrival, expiry, ttl in batch:
                heappush(events, (arrival, domain, expiry, ttl))
        safe_before = min(bound for _, bound in reports.values())
        del self.prefetch_reports[key]
        prefetcher = self.prefetchers[cluster]
        while events and events[0][0] < safe_before:
            arrival, domain, expiry, ttl = heappop(events)
            prefetcher.observe(domain, arrival, expiry, ttl)
        now = self.clock.now
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
        if final or now >= end or now < self.prefetch_next_advice[cluster]:
            return
        advice_period = self.cfg["prefetch"]["advice_seconds"]
        self.prefetch_next_advice[cluster] = (floor(now / advice_period) + 1) * advice_period
        manager.stats["prefetch_advice_runs"] += 1
        advice = prefetcher.advise(now)
        manager.stats["prefetch_advised"] += len(advice)
        assigned = defaultdict(int)
        maximum = self.cfg["prefetch"]["max_per_forwarder"]
        # Domain shards receive disjoint portions of each forwarder's budget.
        quota = maximum // self.manager_shards + (self.manager_shard < maximum % self.manager_shards)
        for deadline, domain in advice:
            if (cluster, domain) in self.prefetch_inflight:
                continue
            candidates = [member for member in manager.members if assigned[member] < quota]
            if not candidates:
                break
            # Rotating deterministic tie-break avoids all domain shards choosing ID 0.
            target = min(candidates, key=lambda member: (
                manager.loads.get(member, float("inf")) + assigned[member],
                (member - domain) % self.cfg["num_forwarders"]))
            due = max(now, deadline - self.cfg["forwarder_authoritative_RTT"] / 1000)
            if due >= end:
                continue
            assigned[target] += 1
            self.prefetch_inflight[(cluster, domain)] = now
            manager.stats["prefetch_tasks_sent"] += 1
            self._send("forwarder", ("prefetch_task", target, domain, due, measure))

    def _execute_prefetch(self, node_id, domain, measure):
        node = self._select(node_id, measure)
        key = (node_id, domain)
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]

        def result(expiry):
            self._send("manager", ("prefetch_result", node_id // self.clusters.size,
                                   node_id, domain, expiry, measure))

        if (self.clock.now >= end or not node.capacity or key in self.prefetch_fetching or
                self.prefetch_active[node_id] >= self.cfg["prefetch"]["max_per_forwarder"]):
            node.stats["prefetch_skipped"] += 1
            result(None)
            return
        node.stats["prefetch_started"] += 1
        self.prefetch_fetching.add(key)
        self.prefetch_active[node_id] += 1

        def completed(answer):
            expiry, ttl, _ = answer
            current = self._select(node_id, measure)
            current.stats["prefetch_completed"] += 1
            if current.insert_cache(Record(domain, expiry, ttl)):
                current.stats["prefetch_stored"] += 1
                current.cache[domain].prefetch_stats = current.stats
            self.prefetch_fetching.remove(key)
            self.prefetch_active[node_id] -= 1
            result(expiry)

        token = self._new_token()
        self.pending[token] = completed
        # Force an authoritative refresh, even when the old entry is still live.
        self._send("authority", ("prefetch_query", self.endpoint or self.role,
                                 token, domain, measure))

    def _prefetch_message(self, message):
        kind = message[0]
        if kind == "prefetch_report":
            self._receive_prefetch_report(message)
        elif kind == "prefetch_task":
            _, node_id, domain, due, measure = message
            self._select(node_id, measure).stats["prefetch_tasks_received"] += 1
            heappush(self.prefetch_queue, (due, node_id, domain, measure))
        elif kind == "prefetch_query":
            _, sender, token, domain, measure = message
            record = self.authority.lookup(domain, measure=False)
            self.authority.prefetch_queries += int(measure)
            self._send(sender, ("answer", token, (record.expires_at, record.auth_ttl, 0.0)))
        elif kind == "prefetch_result":
            _, cluster, forwarder, domain, expiry, measure = message
            self._select_manager(cluster, measure)._check_member(forwarder)
            issued_at = self.prefetch_inflight.pop((cluster, domain), None)
            if expiry is not None:
                prefetcher = self.prefetchers[cluster]
                if issued_at is not None and domain in prefetcher.tracking:
                    prefetcher.mark_advised(domain, issued_at)
                prefetcher.refreshed(domain, expiry)
        else:
            return False
        return True
