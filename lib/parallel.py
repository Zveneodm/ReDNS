"""Deterministic ownership and IPC routing; no cache state is replicated."""

from src.manager import empty_manager_metrics
from src.variants import PREFETCH_MODELS
from lib.metrics import aggregate


ROLES = ("client", "forwarder", "authority")
FORWARDER_TOTALS = (
    "successful_reverse_queries", "cluster_queries", "cluster_hits", "cluster_misses",
    "cluster_forwarder_hits", "cluster_client_hits", "manager_queries",
    "manager_directory_hits", "manager_directory_misses",
)


def simulation_roles(config, model):
    if model in PREFETCH_MODELS or (model == "ReDNS" and config["forwarder_cluster_size"] > 1
                                      and config["num_forwarders"] > 1):
        return (*ROLES, "manager")
    return ROLES


class WorkerLayout:
    """Node i belongs to shard i % count, independent of the experiment model."""

    def __init__(self, config, model):
        self.config = config
        clusters = ((config["num_forwarders"] + config["forwarder_cluster_size"] - 1)
                    // config["forwarder_cluster_size"])
        self.counts = {role: min(config["workers"][role], count) for role, count in (
            ("client", config["num_clients"]), ("forwarder", config["num_forwarders"]),
            ("manager", clusters * config["num_domains"]))}
        self.counts["authority"] = 1
        self.roles = simulation_roles(config, model)
        self.workers = {}
        self.driver = "driver" if self.counts["client"] > 1 else "client"
        if self.driver == "driver":
            self.workers["driver"] = dict(role="driver", shard=0, shards=1, node_ids=())
        for role in self.roles:
            total = (config["num_clients"] if role == "client" else
                     config["num_forwarders"] if role == "forwarder" else
                     clusters if role == "manager" else 1)
            for shard in range(self.counts[role]):
                name = self.name(role, shard)
                # Manager workers share static cluster membership, but every
                # (cluster, domain) directory entry has exactly one owner.
                ids = range(total) if role == "manager" else range(shard, total, self.counts[role])
                self.workers[name] = dict(role=role, shard=shard, shards=self.counts[role], node_ids=ids)

    def name(self, role, shard):
        return role if self.counts[role] == 1 else f"{role}:{shard}"

    def owner(self, role, node, domain=None):
        if role == "manager":
            if domain is None:
                raise ValueError("manager ownership requires a domain")
            node = node * self.config["num_domains"] + domain
        name = self.name(role, node % self.counts[role])
        if name not in self.workers:
            raise ValueError(f"inactive destination: {name}")
        return name

    def destination(self, role, message):
        if role not in (*ROLES, "manager"):
            if role not in self.workers:
                raise ValueError(f"unknown worker: {role}")
            return role
        kind = message[0]
        if kind in ("prefetch_local_report", "prefetch_local_result"):
            return self.name("manager", message[1] % self.counts["manager"])
        if kind == "prefetch_local_grant":
            return self.owner("forwarder", message[1])
        if kind == "prefetch_report":
            return self.name("manager", message[3])
        if kind == "prefetch_result":
            return self.owner("manager", message[1], message[3])
        if kind == "prefetch_dispatch":
            return self.owner("manager", message[1], message[3])
        if kind in ("prefetch_candidates", "prefetch_budget_result"):
            return self.name("manager", message[1] % self.counts["manager"])
        if kind == "prefetch_task":
            return self.owner("forwarder", message[1])
        if kind == "prefetch_query":
            return self.owner("authority", 0)
        if kind in ("answer", "query_answer", "peer_answer", "manager_answer", "cluster_answer"):
            token = message[1]
            if not isinstance(token, tuple) or token[0] not in self.workers:
                raise ValueError("reply has no active token owner")
            return token[0]
        if kind == "query":
            node = message[3]
        elif role == "manager":
            # The same domain shard receives its updates, queries and feedback.
            domain = message[3] if kind == "region_update" else message[4]
            return self.owner("manager", message[1], domain)
        elif kind == "cluster_query":
            node = message[3]
        elif kind == "peer_query":
            node = message[2]
        elif kind in ("cache_stored", "cache_removed", "store"):
            node = message[1]
        else:
            raise ValueError(f"cannot route {kind} to {role}")
        return self.owner(role, node)


def merge_snapshots(config, model, snapshots):
    """Reassemble disjoint owners in global node order, then compute rates."""
    result = {}
    for snapshot in snapshots.values():
        # Latency arrays/fingerprints live only on the one arrival driver.
        for key, value in snapshot.items():
            if key not in {"client", "forwarder", "manager", "clients", "forwarders", "managers",
                           "forwarder_clusters", "client_retention", "forwarder_retention", *FORWARDER_TOTALS}:
                result[key] = value
    for role, plural, id_field, expected in (
        ("client", "clients", "client_id", config["num_clients"]),
        ("forwarder", "forwarders", "forwarder_id", config["num_forwarders"]),
    ):
        rows = sorted((row for snap in snapshots.values() for row in snap.get(plural, ())),
                      key=lambda row: row[id_field])
        if [row[id_field] for row in rows] != list(range(expected)):
            raise RuntimeError(f"missing or duplicated {role} ownership")
        result[plural], result[role] = rows, aggregate(rows)
    partials = [row for snap in snapshots.values() for row in snap.get("managers", ())]
    cluster_count = ((config["num_forwarders"] + config["forwarder_cluster_size"] - 1)
                     // config["forwarder_cluster_size"])
    expected_managers = list(range(cluster_count)) if "manager" in simulation_roles(config, model) else []
    manager_parts = WorkerLayout(config, model).counts["manager"]
    managers = []
    for cluster in expected_managers:
        parts = [row for row in partials if row["cluster_id"] == cluster]
        if len(parts) != manager_parts:
            raise RuntimeError("missing or duplicated manager shard")
        members = parts[0]["forwarders"]
        if any(row["forwarders"] != members for row in parts):
            raise RuntimeError("manager shards disagree about cluster membership")
        row = dict(cluster_id=cluster, forwarders=members)
        row.update({key: sum(part[key] for part in parts) for key in empty_manager_metrics()})
        managers.append(row)
    if len(partials) != len(expected_managers) * manager_parts:
        raise RuntimeError("unexpected manager shard")
    result["managers"], result["manager"] = managers, empty_manager_metrics()
    for row in managers:
        for key in result["manager"]:
            result["manager"][key] += row[key]
    for key in FORWARDER_TOTALS:
        result[key] = result["forwarder"][key]
    size = config["forwarder_cluster_size"]
    result["forwarder_clusters"] = ([list(range(i, min(i + size, config["num_forwarders"])))
                                    for i in range(0, config["num_forwarders"], size)]
                                   if model != "baseline" else [])
    return result
