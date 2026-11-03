"""Process-owned DNS caches and cluster directories using bounded IPC queues."""

from array import array
from collections import deque
from hashlib import sha256
import json
from math import fsum
import multiprocessing as mp
from multiprocessing.connection import wait
import os
from queue import Empty, Full
import random
from struct import pack
from time import get_clock_info, perf_counter, perf_counter_ns, sleep
import traceback

from sim.traffic.arrivals import Workload
from lib.clock import RealtimeClock
from lib.config import validate_config
from lib.progress import Progress
from lib.metrics import aggregate, validate_results
from lib.parallel import ROLES, WorkerLayout, merge_snapshots, simulation_roles
from lib.timing import SimulationOverload, TimingMonitor
from src.protocol import DNSProtocol
from src.manager import empty_manager_metrics
from src.variants import COOPERATIVE_MODELS, MODELS, PREFETCH_MODELS


class _Layer(DNSProtocol):
    def __init__(self, role, model, config, inboxes, control, cpu, *, layout=None, endpoint=None):
        self.layout = layout
        self.arrival_driver = role == "client" if layout is None else endpoint == layout.driver
        self.dispatch_driver = role == "driver"
        owned_ids = None if layout is None else layout.workers[endpoint]["node_ids"]
        role = "client" if self.dispatch_driver else role
        self.role, self.model, self.cfg = role, model, config
        self.inboxes, self.control = inboxes, control
        self.cpu = None
        if cpu is not None and hasattr(os, "sched_setaffinity"):
            os.sched_setaffinity(0, {cpu})
            self.cpu = cpu
        self.outbox = deque()
        self.sent = self.received = 0
        self.last_activity_ns = 0
        self.running, self.paused, self.started = True, False, False
        self.retention_observer = None
        ttls = ()
        if role == "authority":
            rng = random.Random(config["seed"] + 1)
            distribution = config["ttl_distribution"]
            ttls = (rng.choices(distribution["values"], weights=distribution["probabilities"],
                                k=config["num_domains"]) if distribution is not None else
                    [rng.randint(*config["ttl_range"]) for _ in range(config["num_domains"])])
        super().__init__(role, model, config, RealtimeClock(), ttls=ttls,
                         node_ids=owned_ids, endpoint=endpoint,
                         manager_shards=1 if layout is None else layout.counts["manager"],
                         manager_shard=0 if layout is None or role != "manager" else
                         layout.workers[endpoint]["shard"])
        if role == "authority":
            self.ttl_fingerprint = sha256(json.dumps(ttls, separators=(",", ":")).encode()).hexdigest()
        if self.arrival_driver:
            self.workload = Workload(config)
            self.in_flight = 0
            self.timing = TimingMonitor(config["timing"])
            self.workload_fingerprint = sha256()
            self.overload_reported = False
            self.driver_done = False
            self.latencies = array("d")
            self.authority_requests = array("B")
            self.wall_latencies = array("d") if config["record_wall_latencies"] else None
            self.wall_latency_total = self.wall_latency_max = 0.0
            self.arrival_lag_total = self.arrival_lag_max = 0.0
            self.warmup_queries = 0
            self.last_progress_ns = 0

    def _send(self, role, message):
        if self.layout is not None:
            role = self.layout.destination(role, message)
        self.outbox.append((role, message))

    def _flush(self):
        # A full destination must not block replies to a different layer.
        blocked = set()
        deferred = []
        sent = 0
        for _ in range(len(self.outbox)):
            if sent >= 256:
                break
            role, message = self.outbox.popleft()
            if role in blocked:
                deferred.append((role, message))
                continue
            try:
                self.inboxes[role].put_nowait(message)
            except Full:
                blocked.add(role)
                deferred.append((role, message))
            else:
                self.sent += 1
                sent += 1
        # Deferred messages precede the unvisited tail, including when a
        # destination has more than one flush batch of pending messages.
        self.outbox.extendleft(reversed(deferred))

    def _message(self, message):
        self.received += 1
        self.last_activity_ns = perf_counter_ns()
        self.handle_message(message)

    def _issue(self, client, domain, scheduled):
        trace = {}
        measure = scheduled >= self.cfg["warm_up_seconds"]
        issued_ns = perf_counter_ns()
        lag_sim = max(0.0, (issued_ns - self.clock.start_ns) / (1e9 * self.clock.scale) - scheduled)
        self.timing.observe(scheduled, lag_sim)
        self.workload_fingerprint.update(pack("!dQQ", scheduled, client, domain))
        if self.timing.sustained and not self.overload_reported:
            self.overload_reported = True
            self.control.send(("timing_overload", self.timing.report()))
        index = None
        if measure:
            index = len(self.latencies)
            self.latencies.append(0)
            self.authority_requests.append(0)
            if self.wall_latencies is not None:
                self.wall_latencies.append(0)
            lag = lag_sim * self.clock.scale * 1000
            self.arrival_lag_total += lag
            self.arrival_lag_max = max(self.arrival_lag_max, lag)
        else:
            self.warmup_queries += 1
        self.in_flight += 1

        def completed(answer):
            self.in_flight -= 1
            self.last_activity_ns = perf_counter_ns()
            if measure:
                wall_ms = (perf_counter_ns() - issued_ns) / 1e6
                self.latencies[index] = answer[2]
                self.authority_requests[index] = int(trace["authority_query"])
                if self.wall_latencies is not None:
                    self.wall_latencies[index] = wall_ms
                self.wall_latency_total += wall_ms
                self.wall_latency_max = max(self.wall_latency_max, wall_ms)

        if self.dispatch_driver:
            # Runtime dispatch/reply messages add no synthetic DNS RTT. They
            # keep one global schedule and in-flight cap across client owners.
            token = self._new_token()
            def dispatched(answer, authority_query):
                trace["authority_query"] = authority_query
                completed(answer)

            self.pending[token] = dispatched
            self._send("client", ("query", self.endpoint, token, client, domain, True, measure, None))
        else:
            self._query(client, domain, True, measure, completed, trace=trace)

    def _drive(self):
        if not self.arrival_driver:
            return
        for _ in range(128):
            if self.in_flight >= self.cfg["max_in_flight"]:
                break
            if self.started and not self.driver_done:
                end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
                if self.workload.next_time >= end:
                    if self.in_flight == 0 and self.clock.now >= end:
                        self.driver_done = True
                        self.last_activity_ns = perf_counter_ns()
                        self.control.send(("driver_done", None))
                    break
                if self.clock.now < self.workload.next_time:
                    break
                when, client, domain = self.workload.pop()
                self._issue(client, domain, when)
            else:
                break
        now_ns = perf_counter_ns()
        if self.started and not self.driver_done and now_ns - self.last_progress_ns > 1_000_000_000:
            self.last_progress_ns = now_ns
            # Show how far the arrival schedule has actually been dispatched,
            # rather than letting wall-clock backlog prematurely show 100%.
            when = min(self.clock.now, self.workload.next_time)
            self.control.send(("progress", when))

    def _status(self):
        return dict(pending=len(self.pending) + len(self.prefetch_queue) + len(self.inflight_queries),
                    outbox=len(self.outbox), sent=self.sent,
                    received=self.received, last_activity_ns=self.last_activity_ns)

    def _snapshot(self):
        if self.dispatch_driver:
            return self._arrival_snapshot()
        if self.role == "authority":
            return {"authoritative_queries": self.authority.queries,
                    "prefetch_authoritative_queries": self.authority.prefetch_queries,
                    "ttl_fingerprint_sha256": self.ttl_fingerprint}
        if self.role == "manager":
            rows = [{"cluster_id": cluster_id, "forwarders": sorted(manager.members),
                     **self._select_manager(cluster_id, True).metrics()}
                    for cluster_id, manager in self.managers.items()]
            totals = empty_manager_metrics()
            for row in rows:
                for field in totals:
                    totals[field] += row[field]
            return {"manager": totals, "managers": rows}
        rows = []
        for index in self.node_ids:
            row = self._select(index, True).metrics()
            if self.role == "client":
                row.update(client_id=index, forwarder_id=index % self.cfg["num_forwarders"])
            elif self.role == "forwarder":
                row["forwarder_id"] = index
                if self.model != "baseline":
                    row["cluster_id"] = index // self.cfg["forwarder_cluster_size"]
            rows.append(row)
        result = {self.role: aggregate(rows)}
        if self.role == "forwarder":
            for field in ("successful_reverse_queries", "cluster_queries", "cluster_hits", "cluster_misses",
                          "cluster_forwarder_hits", "cluster_client_hits", "manager_queries",
                          "manager_directory_hits", "manager_directory_misses"):
                result[field] = result["forwarder"][field]
        if self.role in ("client", "forwarder"):
            result["clients" if self.role == "client" else "forwarders"] = rows
        if self.arrival_driver:
            result.update(self._arrival_snapshot())
        if self.retention_observer is not None:
            result[f"{self.role}_retention"] = self.retention_observer.snapshot()
        return result

    def _arrival_snapshot(self):
        n = len(self.latencies)
        total = fsum(self.latencies)
        result = dict(
            workload_fingerprint_sha256=self.workload_fingerprint.hexdigest(),
            timing_validation=self.timing.report(finish=True),
            total_queries=n, warm_up_queries=self.warmup_queries,
            request_latencies_ms=self.latencies, total_latency_ms=total,
            request_authority_queries=self.authority_requests,
            average_latency_ms=total / n if n else 0.0,
            average_wall_latency_ms=self.wall_latency_total / n if n else 0.0,
            max_wall_latency_ms=self.wall_latency_max,
            arrival_lag_mean_wall_ms=self.arrival_lag_total / n if n else 0.0,
            arrival_lag_max_wall_ms=self.arrival_lag_max,
        )
        if self.wall_latencies is not None:
            result["request_wall_latencies_ms"] = self.wall_latencies
        return result

    def _command(self, command):
        kind, *args = command
        if kind == "start":
            self.clock.start(*args)
            self.last_activity_ns = self.clock.start_ns
            self.control.send(("started", None))
        elif kind == "go":
            self.started = True
            self.last_progress_ns = perf_counter_ns()
            self.control.send(("running", None))
        elif kind == "pause":
            self.paused = True
            self.control.send(("paused", self._status()))
        elif kind == "resume":
            self.paused = False
            self.control.send(("resumed", None))
        elif kind == "snapshot":
            self.clock.freeze(args[0])
            self.control.send(("snapshot", self._snapshot()))
        elif kind == "stop":
            self.running = False
        else:
            raise ValueError(f"unknown command: {kind}")

    def loop(self):
        inbox = self.inboxes[self.endpoint or self.role]
        while self.running:
            while self.control.poll():
                self._command(self.control.recv())
            if not self.running:
                break
            if self.paused:
                sleep(0.001)
                continue
            self._flush()
            worked = False
            for _ in range(128):
                try:
                    message = inbox.get_nowait()
                except Empty:
                    break
                self._message(message)
                worked = True
            self._drive()
            if self.started:
                self.prefetch_tick()
            self._flush()
            if worked or self.outbox:
                continue
            timeout = 0.005
            if self.arrival_driver and self.started and not self.driver_done:
                end = self.cfg["warm_up_seconds"] + self.cfg["duration_seconds"]
                due = min(self.workload.next_time, end)
                delay = (due - self.clock.now) * self.clock.scale
                if delay < 0.0005 and self.in_flight < self.cfg["max_in_flight"]:
                    continue
                timeout = min(timeout, max(0.0001, delay - 0.00025))
            try:
                self._message(inbox.get(timeout=timeout))
            except Empty:
                pass


def _worker(endpoint, model, config, inboxes, control, cpu, layout, observation=None):
    try:
        spec = layout.workers[endpoint]
        layer = _Layer(spec["role"], model, config, inboxes, control, cpu,
                       layout=layout, endpoint=endpoint)
        if observation is not None and not layer.dispatch_driver:
            from lib.retention import attach_observer

            layer.retention_observer = attach_observer(layer, observation)
        control.send(("ready", {"pid": os.getpid(), "cpu": layer.cpu,
                                "role": spec["role"], "shard": spec["shard"],
                                "shards": spec["shards"], "node_count": len(spec["node_ids"])}))
        layer.loop()
    except BaseException:
        try:
            control.send(("error", traceback.format_exc()))
        except (OSError, EOFError):
            pass
        raise
    finally:
        control.close()
        for inbox in inboxes.values():
            inbox.cancel_join_thread()


def available_cpu_pool(config):
    """Reserve the same role slots for both models, including the manager slot."""
    cpus = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
    return tuple(cpus[:len(WorkerLayout(config, "ReDNS-prefetch").workers)]) if config["pin_cpu"] else ()


class RealtimeSimulation:
    def __init__(self, config, model="baseline", *, observation=None,
                 cpu_pool=None, abort_on_overload=False):
        if model not in MODELS:
            raise ValueError("unknown model")
        self.config = validate_config(config)
        if self.config["time_scale"] == "auto":
            raise ValueError("independent auto calibration is disabled; use "
                             "python3 -m sim.run_comparison --time-scale auto")
        self.model = model
        self.roles = simulation_roles(self.config, model)
        self.layout = WorkerLayout(self.config, model)
        self.endpoints = tuple(self.layout.workers)
        self.observation = observation
        self.cpu_pool = available_cpu_pool(self.config) if cpu_pool is None else tuple(cpu_pool)
        self.abort_on_overload = abort_on_overload
        self.processes = {}
        self.controls = {}
        self.reports = {}
        self.progress = None

    def _receive(self, expected, roles=None, timeout=None):
        roles = self.endpoints if roles is None else roles
        remaining = set(roles)
        result = {}
        buffered = self.reports.setdefault(expected, {})
        for role in tuple(remaining):
            if role in buffered:
                result[role] = buffered.pop(role)
                remaining.remove(role)
        started = perf_counter()
        reverse = {conn: role for role, conn in self.controls.items()}
        while remaining:
            if timeout is not None and perf_counter() - started > timeout:
                raise RuntimeError(f"timeout waiting for {expected}: {sorted(remaining)}")
            for conn in wait(list(reverse), timeout=0.2):
                role = reverse[conn]
                try:
                    kind, payload = conn.recv()
                except EOFError as exc:
                    raise RuntimeError(f"{role} process exited unexpectedly") from exc
                if kind == "error":
                    raise RuntimeError(f"{role} process failed:\n{payload}")
                if kind == "progress":
                    self.progress.update(payload)
                elif kind == "timing_overload":
                    if self.abort_on_overload:
                        raise SimulationOverload(payload)
                elif kind == expected and role in remaining:
                    remaining.remove(role)
                    result[role] = payload
                else:
                    self.reports.setdefault(kind, {})[role] = payload
            for role, process in self.processes.items():
                if process.exitcode is not None:
                    raise RuntimeError(f"{role} process exited with code {process.exitcode}")
        return result

    def _broadcast(self, command):
        for connection in self.controls.values():
            connection.send(command)

    def run(self):
        cfg = self.config
        context = mp.get_context("spawn")
        inboxes = {name: context.Queue(maxsize=4 * cfg["max_in_flight"] + 256) for name in self.endpoints}
        cpus = self.cpu_pool
        total_started = perf_counter()
        self.progress = Progress(cfg["warm_up_seconds"] + cfg["duration_seconds"])
        self.progress.update(0)
        try:
            # Baseline leaves the manager slots idle; common owners receive
            # the same CPU even when their worker counts differ from one.
            canonical = list(WorkerLayout(cfg, "ReDNS-prefetch").workers)
            for role in self.endpoints:
                index = canonical.index(role)
                parent, child = context.Pipe()
                cpu = cpus[index % len(cpus)] if cfg["pin_cpu"] and cpus else None
                process = context.Process(target=_worker,
                    args=(role, self.model, cfg, inboxes, child, cpu, self.layout, self.observation),
                    name=f"DNS-{role}")
                process.start()
                child.close()
                self.controls[role], self.processes[role] = parent, process
            workers = self._receive("ready", timeout=120)
            scale = cfg["time_scale"]
            lead_ns = 100_000_000
            while True:
                armed_ns = perf_counter_ns()
                epoch = armed_ns + lead_ns
                self._broadcast(("start", epoch, scale))
                self._receive("started", timeout=30)
                if perf_counter_ns() < epoch:
                    break
                lead_ns = max(lead_ns * 2, 2 * (perf_counter_ns() - armed_ns))
            self._broadcast(("go",))
            self._receive("running", timeout=30)
            self._receive("driver_done", (self.layout.driver,))
            # Freeze all owners to take a consistent distributed cut. Counters
            # include accepted queue writes, so feeder-buffered messages count
            # as in transit. Resume until requests AND background stores drain.
            while True:
                self._broadcast(("pause",))
                statuses = self._receive("paused", timeout=30)
                if (all(not row["pending"] and not row["outbox"] for row in statuses.values())
                        and sum(row["sent"] for row in statuses.values()) ==
                        sum(row["received"] for row in statuses.values())):
                    break
                self._broadcast(("resume",))
                self._receive("resumed", timeout=30)
                sleep(0.005)
            horizon_ns = epoch + int((cfg["warm_up_seconds"] + cfg["duration_seconds"]) * scale * 1e9)
            stop_ns = max(horizon_ns, *(row["last_activity_ns"] for row in statuses.values()))
            self._broadcast(("snapshot", stop_ns))
            snapshots = self._receive("snapshot", timeout=120)
            result = dict(
                schema_version=2, topology="client-forwarder-authority",
                experiment=self.model, config=cfg, execution_mode="realtime",
                manager_enabled="manager" in self.roles,
                cooperation_enabled=self.model in COOPERATIVE_MODELS,
                prefetch_enabled=self.model in PREFETCH_MODELS,
                prefetch_scope=("forwarder" if self.model == "baseline-prefetch" else
                                "cluster" if self.model == "ReDNS-prefetch" else "disabled"),
                prefetch_budget_scope="cluster",
                prefetch_budget_basis="forwarder_foreground_queries",
                manager=empty_manager_metrics(), managers=[],
                clock_basis="scaled_monotonic_wall_time",
                network_latency_model="accumulated_rtt",
                query_coalescing="per_forwarder_domain",
                coalesced_wait_model="remaining_uncompleted_link_rtt",
                ipc_messages_sent=sum(row["sent"] for row in statuses.values()),
                ipc_messages_received=sum(row["received"] for row in statuses.values()),
                elapsed_seconds=cfg["duration_seconds"], warm_up_elapsed_seconds=cfg["warm_up_seconds"],
                latency_unit="ms", arrival_process="poisson", workers=workers,
                clock_resolution_wall_seconds=get_clock_info("perf_counter").resolution,
                actual_time_scale=scale, cpu_pool=list(cpus),
                wall_run_seconds=(stop_ns - epoch) / 1e9,
                observed_simulated_seconds=(stop_ns - epoch) / (scale * 1e9),
            )
            result.update(merge_snapshots(cfg, self.model, snapshots))
            result["authoritative_queries_total"] = (result["authoritative_queries"] +
                                                     result["prefetch_authoritative_queries"])
            if self.observation is not None:
                from lib.retention import merge_retention

                for role in ("client", "forwarder"):
                    parts = [snap[f"{role}_retention"] for snap in snapshots.values()
                             if f"{role}_retention" in snap]
                    result[f"{role}_retention"] = merge_retention(parts, self.model, role,
                                                                 self.observation["output_dir"])
            result["arrival_lag_mean_sim_seconds"] = result["arrival_lag_mean_wall_ms"] / (1000 * scale)
            timing = result["timing_validation"]
            overrun = max(0.0, result["observed_simulated_seconds"] -
                          cfg["warm_up_seconds"] - cfg["duration_seconds"])
            timing["overrun_seconds"] = overrun
            if overrun > cfg["timing"]["max_overrun_seconds"]:
                timing["reasons"].append(f"run overran the arrival horizon by {overrun:.6f}s")
            result["timing_valid"] = timing["valid"] = not timing["reasons"]
            result["wall_total_seconds"] = perf_counter() - total_started
            validate_results(result)
            self.progress.finish()
            return result
        finally:
            self.progress.close()
            for role, connection in self.controls.items():
                if self.processes[role].is_alive():
                    try:
                        connection.send(("stop",))
                    except (OSError, EOFError):
                        pass
            for process in self.processes.values():
                process.join(timeout=5)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            for connection in self.controls.values():
                connection.close()
            for inbox in inboxes.values():
                inbox.close()
                inbox.cancel_join_thread()
