"""Batched cluster telemetry, manager advice and bounded background fetches."""

from collections import defaultdict
from heapq import heappop, heappush
from math import floor

from src.prefetch import Forestall
from src.prefetch_budget import ClusterPrefetchBudget
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
        self.prefetch_running = defaultdict(int)
        self.prefetch_demand_counts = defaultdict(lambda: [0, 0])
        self.prefetch_demand_pending = defaultdict(dict)
        self.prefetch_observation_token = 0
        self.prefetch_fetching = set()
        self.prefetch_queue = []
        self.prefetch_reports = {}
        self.prefetch_report_epoch = {}
        self.prefetch_event_buffers = defaultdict(list)
        self.prefetchers = {cluster: Forestall(self.cfg["prefetch"]) for cluster in self.managers}
        self.prefetch_next_advice = defaultdict(lambda: self.cfg["prefetch"]["advice_seconds"])
        self.prefetch_inflight = {}
        self.prefetch_budget_batches = {}
        self.prefetch_budget_epoch = defaultdict(int)
        self.prefetch_budgets = {
            cluster: ClusterPrefetchBudget(self.cfg["prefetch"]["budget_ratio"], manager.members,
                                           self.cfg["prefetch"]["max_inflight_per_forwarder"],
                                           self.cfg["num_forwarders"])
            for cluster, manager in self.managers.items()
            if cluster % manager_shards == manager_shard}

    def _demand_started(self, node_id, domain, done, *, measure=None):
        if not self.prefetch_enabled or self.role != "forwarder":
            return done
        arrival = self.clock.now
        if measure is None:
            measure = arrival >= self.cfg["warm_up_seconds"]
        self.prefetch_demand_counts[node_id][measure] += 1
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
        if not self.prefetch_enabled:
            return
        if self.role == "manager":
            for cluster in self.prefetch_budgets:
                self._dispatch_prefetch_budget(cluster)
            return
        if self.role != "forwarder":
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
                counts = tuple(self.prefetch_demand_counts.pop(node_id, [0, 0]))
                for phase, count in enumerate(counts):
                    self._select(node_id, bool(phase)).stats["prefetch_budget_demand_reported"] += count
                self._select(node_id, measure)
                load = (self.prefetch_load.pop(node_id, 0) / interval +
                        len(self.prefetch_demand_pending[node_id]))
                # No later report can contain an arrival older than this bound:
                # unresolved demand replies keep the bound behind that arrival.
                watermark = min(self.prefetch_demand_pending[node_id].values(), default=now)
                for shard, rows in enumerate(parts):
                    self._send("manager", ("prefetch_report", cluster, node_id, shard,
                                           epoch, load, rows, watermark, counts, final, measure))
            self.prefetch_epoch = epoch + 1
            self.prefetch_final = final
        for _ in range(128):
            if not self.prefetch_queue or self.prefetch_queue[0][0] > now:
                break
            _, node_id, domain, measure = heappop(self.prefetch_queue)
            self._execute_prefetch(node_id, domain, measure)

    def _receive_prefetch_report(self, message):
        _, cluster, forwarder, shard, epoch, load, rows, watermark, counts, final, measure = message
        manager = self._select_manager(cluster, measure)
        manager._check_member(forwarder)
        if shard != self.manager_shard:
            raise ValueError("prefetch report reached the wrong domain shard")
        report_key = (cluster, forwarder)
        if epoch <= self.prefetch_report_epoch.get(report_key, 0):
            raise ValueError("duplicate or out-of-order prefetch report")
        self.prefetch_report_epoch[report_key] = epoch
        manager.stats["prefetch_reports_received"] += 1
        manager.stats["prefetch_observations_received"] += len(rows)
        manager.loads[forwarder] = load
        # Credit every report exactly once at the budget owner, even if the
        # final report cohort is incomplete when the arrival horizon ends.
        if cluster in self.prefetch_budgets:
            budget = self.prefetch_budgets[cluster]
            changes = budget.fund(counts)
            budget.loads[forwarder] = load
            for phase, count in enumerate(counts):
                stats = self._select_manager(cluster, bool(phase)).stats
                stats["prefetch_budget_demand"] += count
                stats["prefetch_budget_allowance"] += changes[phase]
            self._select_manager(cluster, measure)
        key = (cluster, epoch)
        reports = self.prefetch_reports.setdefault(key, {})
        if forwarder in reports:
            raise ValueError("duplicate prefetch report")
        reports[forwarder] = (rows, watermark, counts, load)
        if len(reports) != len(manager.members):
            return
        events = self.prefetch_event_buffers[cluster]
        for batch, _, _, _ in reports.values():
            for domain, arrival, expiry, ttl in batch:
                heappush(events, (arrival, domain, expiry, ttl))
        safe_before = min(row[1] for row in reports.values())
        demand = tuple(sum(row[2][phase] for row in reports.values()) for phase in (0, 1))
        loads = {node: row[3] for node, row in reports.items()}
        del self.prefetch_reports[key]
        prefetcher = self.prefetchers[cluster]
        while events and events[0][0] < safe_before:
            arrival, domain, expiry, ttl = heappop(events)
            prefetcher.observe(domain, arrival, expiry, ttl)
        now = self.clock.now
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
        advice = None
        if not final and now < end and now >= self.prefetch_next_advice[cluster]:
            advice_period = self.cfg["prefetch"]["advice_seconds"]
            self.prefetch_next_advice[cluster] = (floor(now / advice_period) + 1) * advice_period
            manager.stats["prefetch_advice_runs"] += 1
            selected = prefetcher.advise(now)
            manager.stats["prefetch_advised"] += len(selected)
            advice = [(domain, deadline, now + advice_period,
                       prefetcher.tracking[domain].expires_at,
                       prefetcher.tracking[domain].intervals.mean)
                      for deadline, domain in selected]
        # Domain tracking stays sharded. Only the budget and the merged task
        # queue have one owner, cluster % manager_shards.
        self._send("manager", ("prefetch_candidates", cluster, shard, epoch,
                               demand, loads, advice))

    def _receive_prefetch_candidates(self, message):
        _, cluster, shard, epoch, counts, loads, advice = message
        if not 0 <= shard < self.manager_shards or cluster not in self.prefetch_budgets:
            raise ValueError("prefetch candidates reached the wrong budget owner")
        if epoch <= self.prefetch_budget_epoch[cluster]:
            raise ValueError("prefetch budget epoch already processed")
        key = (cluster, epoch)
        batches = self.prefetch_budget_batches.setdefault(key, {})
        if shard in batches:
            raise ValueError("duplicate prefetch candidate batch")
        batches[shard] = (counts, loads, advice)
        if len(batches) != self.manager_shards:
            return
        if any(row[0] != counts or row[1] != loads for row in batches.values()):
            raise ValueError("prefetch shards disagree about foreground demand")
        budget = self.prefetch_budgets[cluster]
        now = self.clock.now
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
        for owner, (_, _, candidates) in batches.items():
            if candidates is not None:
                candidates = [row for row in candidates
                              if max(now, row[1] - self.cfg["forwarder_authoritative_RTT"] / 1000) < end]
                budget.offer(owner, candidates, now)
        del self.prefetch_budget_batches[key]
        self.prefetch_budget_epoch[cluster] = epoch
        self._dispatch_prefetch_budget(cluster)
        measure = now >= self.cfg["warm_up_seconds"]
        if budget.pending and budget.available(measure) <= 0 and now < end:
            self._select_manager(cluster, measure).stats["prefetch_budget_blocked"] += 1

    def _dispatch_prefetch_budget(self, cluster):
        budget = self.prefetch_budgets[cluster]
        now = self.clock.now
        if now >= self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]:
            budget.clear_pending()
            return
        measure = now >= self.cfg["warm_up_seconds"]
        for _ in range(128):
            task = budget.take(now, measure)
            if task is None:
                return
            candidate, target = task
            self._select_manager(cluster, measure).stats["prefetch_budget_reserved"] += 1
            self._send("manager", ("prefetch_dispatch", cluster, target, candidate.domain,
                                   candidate.deadline, candidate.valid_until,
                                   candidate.observed_expiry, now, measure))

    def _receive_prefetch_dispatch(self, message):
        _, cluster, target, domain, deadline, valid_until, known_expiry, issued_at, measure = message
        manager = self._select_manager(cluster, measure)
        manager._check_member(target)
        tracker = self.prefetchers[cluster]
        entry = tracker.tracking.get(domain)
        now = self.clock.now
        due = max(now, deadline - self.cfg["forwarder_authoritative_RTT"] / 1000)
        if (entry is None or entry.expires_at > known_expiry or now >= valid_until or
                (cluster, domain) in self.prefetch_inflight or
                due >= self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]):
            self._send("manager", ("prefetch_budget_result", cluster, target, domain, False, measure))
            return
        self.prefetch_inflight[(cluster, domain)] = issued_at
        manager.stats["prefetch_tasks_sent"] += 1
        self._send("forwarder", ("prefetch_task", target, domain, due, measure))

    def _receive_prefetch_budget_result(self, message):
        _, cluster, target, domain, success, measure = message
        budget = self.prefetch_budgets[cluster]
        if budget.inflight.get(domain, ())[:2] != (target, measure):
            raise ValueError("prefetch completion does not match its budget reservation")
        budget.finish(domain, success)
        if not success:
            self._select_manager(cluster, measure).stats["prefetch_budget_refunded"] += 1
        self._dispatch_prefetch_budget(cluster)

    def _execute_prefetch(self, node_id, domain, measure):
        node = self._select(node_id, measure)
        key = (node_id, domain)
        end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]

        def result(expiry):
            self._send("manager", ("prefetch_result", node_id // self.clusters.size,
                                   node_id, domain, expiry, measure))

        if (self.clock.now >= end or not node.capacity or key in self.prefetch_fetching or
                self.prefetch_running[node_id] >= self.cfg["prefetch"]["max_inflight_per_forwarder"]):
            node.stats["prefetch_skipped"] += 1
            result(None)
            return
        node.stats["prefetch_started"] += 1
        self.prefetch_fetching.add(key)
        self.prefetch_active[node_id] += 1
        self.prefetch_running[node_id] += 1

        def completed(answer):
            expiry, ttl, _ = answer
            current = self._select(node_id, measure)
            current.stats["prefetch_completed"] += 1
            if current.insert_cache(Record(domain, expiry, ttl)):
                current.stats["prefetch_stored"] += 1
                current.cache[domain].prefetch_stats = current.stats
            self.prefetch_fetching.remove(key)
            self.prefetch_active[node_id] -= 1
            self.prefetch_running[node_id] -= 1
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
        elif kind == "prefetch_candidates":
            self._receive_prefetch_candidates(message)
        elif kind == "prefetch_dispatch":
            self._receive_prefetch_dispatch(message)
        elif kind == "prefetch_budget_result":
            self._receive_prefetch_budget_result(message)
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
            if issued_at is not None:
                self._send("manager", ("prefetch_budget_result", cluster, forwarder, domain,
                                       expiry is not None, measure))
        else:
            return False
        return True
