"""Four single-owner DNS layers driven by scaled monotonic wall time."""

from array import array
from collections import Counter, deque
from functools import partial
from math import fsum
import multiprocessing as mp
from multiprocessing.connection import wait
import os
from queue import Empty, Full
import random
from time import get_clock_info, perf_counter, perf_counter_ns, sleep
import traceback
from zlib import crc32

from sim.arrivals import Workload
from sim.clock import RealtimeClock
from sim.experiment import validate_config
from sim.progress import Progress
from sim.preferences import PreferenceLearner
from src.baseline.Model import Resolver
from src.Re_DNS.Model import ReDNS_Resolver


ROLES = ("client", "forwarder", "public_dns", "authority")


def _aggregate(nodes):
    result = Counter()
    for node in nodes:
        result.update({key: value for key, value in node.items()
                       if key not in ("hit_rate", "transfer_use_rate", "transfer_waste_rate")})
    result["hit_rate"] = result["hits"] / result["queries"] if result["queries"] else 0.0
    if "transfers_accepted" in result:
        accepted = result["transfers_accepted"]
        result["transfer_use_rate"] = result["transfer_records_used"] / accepted if accepted else 0.0
        unused = sum(result[f"transfer_unused_{reason}"]
                     for reason in ("expired", "evicted", "replaced"))
        result["transfer_waste_rate"] = unused / accepted if accepted else 0.0
    return dict(result)


class _Layer:
    def __init__(self, role, model, config, inboxes, control, cpu):
        self.role, self.model, self.cfg = role, model, config
        self.inboxes, self.control = inboxes, control
        self.cpu = None
        if cpu is not None and hasattr(os, "sched_setaffinity"):
            os.sched_setaffinity(0, {cpu})
            self.cpu = cpu
        self.clock = RealtimeClock()
        self.nodes = []
        self.names = {}
        self.formal_stats = []
        self.warm_stats = {}
        self.pending = {}
        self.outbox = deque()
        self.token = 0
        self.sent = self.received = 0
        self.last_activity_ns = 0
        self.running, self.paused, self.started = True, False, False
        self.authority_queries = 0
        options = config["regional_preferences"]
        self.learner = (PreferenceLearner(options["learning_half_life_seconds"],
                                          options["max_learned_domains_per_child"])
                        if options["enabled"] and role in ("forwarder", "public_dns")
                        else None)
        if role == "authority":
            rng = random.Random(config["seed"] + 1)
            distribution = config["ttl_distribution"]
            self.ttls = (rng.choices(distribution["values"], weights=distribution["probabilities"],
                                     k=config["num_domains"]) if distribution is not None else
                         [rng.randint(*config["ttl_range"]) for _ in range(config["num_domains"])])
        else:
            count, size = {
                "client": (config["num_clients"], config["client_cache_size"]),
                "forwarder": (config["num_forwarders"], config["forwarder_cache_size"]),
                "public_dns": (1, config["pub_cache_size"]),
            }[role]
            for index in range(count):
                node = (ReDNS_Resolver(size, config["min_transfer_ttl"])
                        if model == "ReDNS" else Resolver(size))
                node.clock = self.clock
                if model == "ReDNS":
                    node._transfer_evicted = partial(self._transfer, index)
                    original_remove = node._on_cache_remove

                    def removed(key, record, reason, node_id=index, callback=original_remove):
                        callback(key, record, reason)
                        if role in ("client", "forwarder"):
                            parent_role, parent_node, _ = self._upstream(node_id)
                            domain = int(key[7:-8])
                            self._send(parent_role, ("cache_removed", parent_node, node_id,
                                                     domain, record._expires_at))

                    node._on_cache_remove = removed
                self.nodes.append(node)
                self.formal_stats.append(node.stats)
        if role == "client":
            self.workload = Workload(config)
            self.in_flight = 0
            self.calibration_left = self.calibration_completed = 0
            self.calibration_total = 0
            self.driver_done = False
            self.latencies = array("d")
            self.wall_latencies = array("d") if config["record_wall_latencies"] else None
            self.wall_latency_total = self.wall_latency_max = 0.0
            self.arrival_lag_total = self.arrival_lag_max = 0.0
            self.warmup_queries = 0
            self.last_progress_ns = 0

    def _name(self, domain):
        name = self.names.get(domain)
        if name is None:
            name = f"domain-{domain:05d}.example"
            self.names[domain] = name
        return name

    def _select(self, node_id, measure):
        node = self.nodes[node_id]
        if measure:
            node.stats = self.formal_stats[node_id]
        else:
            stats = self.warm_stats.get(node_id)
            if stats is None:
                stats = Counter({key: 0 for key in self.formal_stats[node_id]})
                self.warm_stats[node_id] = stats
            node.stats = stats
        node._runtime_measure = measure
        return node

    def _send(self, role, message):
        self.outbox.append((role, message))

    def _flush(self):
        # A full destination must not block replies to a different layer.
        for _ in range(min(256, len(self.outbox))):
            role, message = self.outbox.popleft()
            try:
                self.inboxes[role].put_nowait(message)
            except Full:
                self.outbox.append((role, message))
            else:
                self.sent += 1

    def _upstream(self, node_id):
        return {
            "client": ("forwarder", node_id % self.cfg["num_forwarders"], self.cfg["client_forwarder_RTT"]),
            "forwarder": ("public_dns", 0, self.cfg["forwarder_pub_RTT"]),
            "public_dns": ("authority", 0, self.cfg["pub_auth_RTT"]),
        }[self.role]

    def _fetch(self, node_id, domain, fill, measure, foreground, done):
        role, target, _ = self._upstream(node_id)
        self.token += 1
        self.pending[self.token] = done
        self._send(role, ("query", self.role, node_id, self.token, target, domain,
                          fill, measure, foreground))

    def _query(self, node_id, domain, fill, measure, foreground, done):
        if self.role == "authority":
            if measure:
                self.authority_queries += 1
            ttl = self.ttls[domain]
            done((self.clock.now + ttl, ttl, 0.0))
            return
        node = self._select(node_id, measure)
        node.stats["queries"] += 1
        name = self._name(domain)
        record = (node.check_cache(name, foreground=foreground)
                  if self.model == "ReDNS" else node.check_cache(name))
        if record is not None:
            node.stats["hits"] += 1
            done((node.cache[name]._expires_at, record.auth_ttl, 0.0))
            return
        node.stats["misses"] += 1
        node.stats["upstream_queries"] += 1
        _, _, rtt = self._upstream(node_id)

        def received(answer):
            expiry, auth_ttl, latency = answer
            node = self._select(node_id, measure)
            if fill:
                node.insert_cache(name, max(0.0, expiry - self.clock.now), auth_ttl,
                                  expires_at=expiry)
            done((expiry, auth_ttl, latency + (rtt if foreground else 0.0)))

        self._fetch(node_id, domain, fill, measure, foreground, received)

    def _transfer(self, node_id, record):
        if self.role == "public_dns" and self.cfg["forwarder_cache_size"]:
            role = "forwarder"
            targets = range(self.cfg["num_forwarders"])
        elif self.role == "forwarder" and self.cfg["client_cache_size"]:
            role = "client"
            targets = range(node_id, self.cfg["num_clients"], self.cfg["num_forwarders"])
        else:
            return
        if not targets:
            return
        domain = int(record.r_name[7:-8])
        node = self.nodes[node_id]
        measure = node._runtime_measure
        target = None
        score = 0.0
        if self.learner is not None:
            target, score = self.learner.choose(domain, targets, record.ttl, self.clock.now)
        if target is None:
            target = targets[crc32(record.r_name.encode("utf-8")) % len(targets)]
            node.stats["preference_fallback_transfers"] += 1
        else:
            node.stats["preference_selected_transfers"] += 1

        def store(answer):
            expiry, auth_ttl, _ = answer
            self._select(node_id, measure).stats["records_transferred"] += 1
            if self.learner is not None:
                self.learner.note_copy(target, domain, expiry)
            self._send(role, ("store", target, domain, expiry, auth_ttl, measure, score))

        if record.ttl < self.cfg["min_transfer_ttl"]:
            node.stats["refresh_queries"] += 1
            node.stats["upstream_queries"] += 1

            def refreshed(answer):
                expiry, auth_ttl, _ = answer
                store((max(expiry, self.clock.now + self.cfg["min_transfer_ttl"]), auth_ttl, 0))

            self._fetch(node_id, domain, False, measure, False, refreshed)
        else:
            store((record._expires_at, record.auth_ttl, 0))

    def _message(self, message):
        self.received += 1
        self.last_activity_ns = perf_counter_ns()
        kind = message[0]
        if kind == "query":
            _, sender, sender_node, token, node, domain, fill, measure, foreground = message
            if self.learner is not None and foreground:
                self.learner.observe(sender_node, domain, self.clock.now)

            def reply(answer):
                downstream_cache = {
                    "forwarder": self.cfg["client_cache_size"],
                    "public_dns": self.cfg["forwarder_cache_size"],
                }.get(self.role, 0)
                if self.learner is not None and fill and downstream_cache:
                    self.learner.note_copy(sender_node, domain, answer[0])
                self._send(sender, ("answer", token, answer))

            self._query(node, domain, fill, measure, foreground, reply)
        elif kind == "answer":
            _, token, answer = message
            self.pending.pop(token)(answer)
        elif kind == "store":
            _, node_id, domain, expiry, auth_ttl, measure, _score = message
            node = self._select(node_id, measure)
            node.receive_transfer(self._name(domain), max(0.0, expiry - self.clock.now),
                                  auth_ttl, expires_at=expiry)
        elif kind == "cache_removed":
            _, _node_id, child_id, domain, removed_expiry = message
            if self.learner is not None:
                self.learner.forget_copy(child_id, domain, removed_expiry)
        else:
            raise ValueError(f"unknown message: {kind}")

    def _issue(self, client, domain, scheduled=None):
        calibration = scheduled is None
        measure = not calibration and scheduled >= self.cfg["warm_up_seconds"]
        issued_ns = perf_counter_ns()
        index = None
        if measure:
            index = len(self.latencies)
            self.latencies.append(0)
            if self.wall_latencies is not None:
                self.wall_latencies.append(0)
            lag = max(0.0, (issued_ns - self.clock.start_ns) / 1e6 - scheduled * self.clock.scale * 1000)
            self.arrival_lag_total += lag
            self.arrival_lag_max = max(self.arrival_lag_max, lag)
        elif not calibration:
            self.warmup_queries += 1
        self.in_flight += 1

        def completed(answer):
            self.in_flight -= 1
            self.last_activity_ns = perf_counter_ns()
            if calibration:
                self.calibration_completed += 1
                if self.calibration_completed == self.calibration_total:
                    elapsed = (perf_counter_ns() - self.calibration_start_ns) / 1e9
                    self.control.send(("calibrated", self.calibration_total / elapsed))
            elif measure:
                wall_ms = (perf_counter_ns() - issued_ns) / 1e6
                self.latencies[index] = answer[2]
                if self.wall_latencies is not None:
                    self.wall_latencies[index] = wall_ms
                self.wall_latency_total += wall_ms
                self.wall_latency_max = max(self.wall_latency_max, wall_ms)

        self._query(client, domain, not calibration, measure, not calibration, completed)

    def _drive(self):
        if self.role != "client":
            return
        for _ in range(128):
            if self.in_flight >= self.cfg["max_in_flight"]:
                break
            if self.calibration_left:
                number = self.calibration_total - self.calibration_left
                self.calibration_left -= 1
                self._issue(number % self.cfg["num_clients"], number % self.cfg["num_domains"])
            elif self.started and not self.driver_done:
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
        return dict(pending=len(self.pending), outbox=len(self.outbox), sent=self.sent,
                    received=self.received, last_activity_ns=self.last_activity_ns)

    def _snapshot(self):
        if self.role == "authority":
            return {"authoritative_queries": self.authority_queries}
        rows = []
        for index in range(len(self.nodes)):
            row = self._select(index, True).metrics()
            if self.role == "client":
                row.update(client_id=index, forwarder_id=index % self.cfg["num_forwarders"])
            elif self.role == "forwarder":
                row["forwarder_id"] = index
            rows.append(row)
        aggregate = _aggregate([{key: value for key, value in row.items()
                                 if key not in ("client_id", "forwarder_id")} for row in rows])
        result = {self.role: aggregate}
        if self.role in ("client", "forwarder"):
            result["clients" if self.role == "client" else "forwarders"] = rows
        if self.learner is not None:
            result[f"{self.role}_preference_learning"] = self.learner.metrics(self.clock.now)
        if self.role == "client":
            n = len(self.latencies)
            total = fsum(self.latencies)
            result.update(
                total_queries=n, warm_up_queries=self.warmup_queries,
                request_latencies_ms=self.latencies, total_latency_ms=total,
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
        if kind == "calibrate":
            self.calibration_total = self.calibration_left = args[0]
            self.calibration_start_ns = perf_counter_ns()
        elif kind == "reset":
            if self.pending or self.outbox:
                raise RuntimeError("calibration was not drained")
            self.sent = self.received = self.authority_queries = 0
            for stats in self.formal_stats:
                for key in stats:
                    stats[key] = 0
            self.warm_stats.clear()
            self.control.send(("reset", None))
        elif kind == "start":
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
        inbox = self.inboxes[self.role]
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
            self._flush()
            if worked or self.outbox:
                continue
            timeout = 0.005
            if self.role == "client" and self.started and not self.driver_done:
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


def _worker(role, model, config, inboxes, control, cpu):
    try:
        layer = _Layer(role, model, config, inboxes, control, cpu)
        control.send(("ready", {"pid": os.getpid(), "cpu": layer.cpu}))
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


class RealtimeSimulation:
    def __init__(self, config, model="baseline"):
        if model not in ("baseline", "ReDNS"):
            raise ValueError("unknown model")
        self.config = validate_config(config)
        self.model = model
        self.processes = {}
        self.controls = {}
        self.reports = {}
        self.progress = None

    def _receive(self, expected, roles=ROLES, timeout=None):
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
        inboxes = {role: context.Queue(maxsize=4 * cfg["max_in_flight"] + 256) for role in ROLES}
        cpus = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
        total_started = perf_counter()
        self.progress = Progress(cfg["warm_up_seconds"] + cfg["duration_seconds"])
        self.progress.update(0)
        try:
            for index, role in enumerate(ROLES):
                parent, child = context.Pipe()
                cpu = cpus[index % len(cpus)] if cfg["pin_cpu"] and cpus else None
                process = context.Process(target=_worker,
                    args=(role, self.model, cfg, inboxes, child, cpu), name=f"DNS-{role}")
                process.start()
                child.close()
                self.controls[role], self.processes[role] = parent, process
            workers = self._receive("ready", timeout=120)
            calibration_rate = None
            scale = cfg["time_scale"]
            if scale == "auto":
                if cfg["queries_per_second"]:
                    self.controls["client"].send(("calibrate", cfg["calibration_requests"]))
                    calibration_rate = self._receive("calibrated", ("client",), timeout=120)["client"]
                    self._broadcast(("reset",))
                    self._receive("reset", timeout=30)
                scale = max(cfg["min_time_scale"],
                            cfg["queries_per_second"] / (0.7 * calibration_rate) if calibration_rate else 0,
                            10 * get_clock_info("perf_counter").resolution * max(1, cfg["queries_per_second"]))
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
            self._receive("driver_done", ("client",))
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
                experiment=self.model, config=cfg, execution_mode="realtime",
                elapsed_seconds=cfg["duration_seconds"], warm_up_elapsed_seconds=cfg["warm_up_seconds"],
                latency_unit="ms", arrival_process="poisson", workers=workers,
                clock_resolution_wall_seconds=get_clock_info("perf_counter").resolution,
                actual_time_scale=scale, calibrated_queries_per_wall_second=calibration_rate,
                wall_run_seconds=(stop_ns - epoch) / 1e9,
                observed_simulated_seconds=(stop_ns - epoch) / (scale * 1e9),
            )
            for snapshot in snapshots.values():
                result.update(snapshot)
            result["arrival_lag_mean_sim_seconds"] = result["arrival_lag_mean_wall_ms"] / (1000 * scale)
            result["wall_total_seconds"] = perf_counter() - total_started
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
