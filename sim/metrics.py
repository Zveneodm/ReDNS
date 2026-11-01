"""Aggregate node counters and verify a fully drained experiment."""

from collections import Counter


RATES = {"hit_rate", "transfer_use_rate", "transfer_waste_rate"}
NODE_IDS = {"client_id", "forwarder_id", "cluster_id"}


def aggregate(nodes):
    result = Counter()
    for node in nodes:
        result.update({key: value for key, value in node.items() if key not in RATES | NODE_IDS})
    result["hit_rate"] = result["hits"] / result["queries"] if result["queries"] else 0.0
    accepted = result["transfers_accepted"]
    result["transfer_use_rate"] = result["transfer_records_used"] / accepted if accepted else 0.0
    wasted = sum(result[f"transfer_unused_{reason}"]
                 for reason in ("expired", "evicted", "replaced"))
    result["transfer_waste_rate"] = wasted / accepted if accepted else 0.0
    return dict(result)


def validate_results(result):
    """Fail before saving if requests, probes, notifications or transfers leak."""
    def equal(actual, expected, name):
        if actual != expected:
            raise RuntimeError(f"inconsistent {name}: {actual} != {expected}")

    client, forwarder = result["client"], result["forwarder"]
    manager = result["manager"]
    equal(forwarder["manager_queries"], manager["queries"], "manager lookup flow")
    equal(forwarder["manager_directory_hits"], manager["directory_hits"], "manager hit flow")
    equal(forwarder["manager_directory_misses"], manager["directory_misses"], "manager miss flow")
    equal(forwarder["manager_updates_sent"], manager["updates_received"], "region notification flow")
    equal(forwarder["manager_feedback_sent"], manager["feedback_received"], "manager feedback flow")
    equal(manager["queries"], manager["directory_hits"] + manager["directory_misses"], "manager decisions")
    equal(manager["updates_received"], manager["updates_applied"] + manager["updates_ignored"],
          "region update outcomes")
    equal(manager["feedback_received"], manager["feedback_applied"] + manager["feedback_ignored"],
          "manager feedback outcomes")
    equal(result["successful_reverse_queries"], forwarder["successful_reverse_queries"],
          "successful reverse query total")
    equal(result["successful_reverse_queries"],
          sum(row["successful_reverse_queries"] for row in result["forwarders"]),
          "per-forwarder reverse query total")
    equal(client["successful_reverse_queries"], 0, "client reverse query count")
    equal(client["queries"], result["total_queries"], "client request count")
    equal(client["upstream_queries"], forwarder["queries"], "client -> forwarder flow")
    equal(forwarder["upstream_queries"], result["authoritative_queries"], "forwarder -> authority flow")
    equal(forwarder["peer_queries"] + forwarder["cluster_child_queries"],
          client["peer_queries_received"], "peer query flow")
    equal(forwarder["peer_hits"] + forwarder["cluster_child_hits"],
          client["peer_hits_served"], "peer hit flow")
    equal(forwarder["cluster_queries"], forwarder["cluster_queries_received"], "cluster query flow")
    equal(forwarder["cluster_hits"], forwarder["cluster_hits_served"], "cluster hit flow")
    equal(forwarder["cluster_misses"], forwarder["cluster_misses_served"], "cluster miss flow")
    equal(forwarder["cluster_forwarder_hits"], forwarder["cluster_cache_hits_served"], "neighbor cache hits")
    equal(forwarder["cluster_client_hits"], forwarder["cluster_child_hits"], "neighbor client hits")
    for field in ("cluster_queries", "cluster_hits", "cluster_misses",
                  "cluster_forwarder_hits", "cluster_client_hits", "manager_queries",
                  "manager_directory_hits", "manager_directory_misses"):
        equal(result[field], forwarder[field], f"{field} total")
    equal(forwarder["records_transferred"], client["records_received"], "transfer flow")
    equal(client["directory_stores_sent"], forwarder["directory_stores_received"], "store notices")
    equal(client["directory_removes_sent"], forwarder["directory_removes_received"], "remove notices")
    for name, row in (("client", client), ("forwarder", forwarder)):
        equal(row["queries"], row["hits"] + row["misses"], f"{name} cache decisions")
        equal(row["upstream_queries"], row["misses"] - row["peer_hits"] - row["cluster_hits"],
              f"{name} upstream count")
        equal(row["peer_queries"], row["peer_hits"] + row["peer_misses"], f"{name} probes")
        equal(row["cluster_queries"], row["cluster_hits"] + row["cluster_misses"], f"{name} cluster probes")
        equal(row["cluster_hits"], row["cluster_forwarder_hits"] + row["cluster_client_hits"],
              f"{name} cluster hit sources")
        equal(row["cluster_child_queries"], row["cluster_child_hits"] + row["cluster_child_misses"],
              f"{name} helper child probes")
        accounted = row["transfer_records_used"] + sum(
            row[f"transfer_unused_{reason}"]
            for reason in ("expired", "evicted", "replaced", "live", "expired_pending"))
        equal(row["transfers_accepted"], accounted, f"{name} transfer cohort")
        equal(row["records_received"], row["transfers_accepted"] + row["transfer_duplicates"]
              + row["transfer_expired_in_transit"] + row["transfers_rejected_full"],
              f"{name} transfer outcomes")
    for row in result["clients"] + result["forwarders"]:
        if not 0 <= row["live_cache_entries"] <= row["cache_entries"] <= row["cache_size"]:
            raise RuntimeError("cache capacity invariant failed")
        if not 0 <= row["successful_reverse_queries"] <= row["peer_hits"] + row["cluster_client_hits"]:
            raise RuntimeError("successful reverse queries must come from a local or neighbor client")
    equal(result["ipc_messages_sent"], result["ipc_messages_received"], "IPC drain")
    if result["experiment"] == "baseline":
        equal(forwarder["manager_queries"], 0, "baseline manager lookups")
        equal(forwarder["manager_updates_sent"], 0, "baseline region advertisements")
        equal(forwarder["cluster_queries"], 0, "baseline cluster probes")
        equal(forwarder["cluster_queries_received"], 0, "baseline cluster assistance")
        equal(forwarder["peer_queries"], 0, "baseline probes")
        equal(forwarder["records_transferred"], 0, "baseline transfers")
        equal(client["directory_stores_sent"], 0, "baseline directory maintenance")
