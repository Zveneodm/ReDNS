"""Optional cache observations; the DNS protocol and cache policies are unchanged.

Only the selected nodes are wrapped. Store/remove callbacks retain their
original behavior. Mutable tracking state is local to each cache-owning process.
"""

import gzip
import json
from pathlib import Path


FIELDS = (
    "queries", "hits", "peer_queries", "peer_hits", "episodes",
    "capacity_evictions", "ttl_expirations", "replacements", "live_at_end",
    "expired_pending_at_end", "live_seconds", "warm_entries", "insertions",
    "expired_removals",
)
INDEX = {name: index for index, name in enumerate(FIELDS)}


class RetentionObserver:
    """Measure cache episodes intersecting [warm-up boundary, snapshot time].

    An episode starts at actual insertion and ends at replacement, eviction,
    expiry or the final snapshot. Lazy expired entries stop contributing live
    time at their TTL deadline, even if their physical deletion happens later.
    Normal query counts retain the experiment's planned-arrival phase tagging.
    """

    def __init__(self, layer, node_ids, output_dir):
        self.layer = layer
        self.node_ids = set(node_ids)
        self.output_dir = Path(output_dir)
        self.start = layer.cfg["warm_up_seconds"]
        self.rows = {node: {} for node in self.node_ids}
        self.active = {node: {} for node in self.node_ids}
        self.context = None
        self.finished = False
        self._install()

    def _row(self, node_id, domain):
        row = self.rows[node_id].get(domain)
        if row is None:
            row = self.rows[node_id][domain] = [0] * len(FIELDS)
        return row

    def _close(self, node_id, domain, end, reason):
        entry, inserted = self.active[node_id].pop(domain)
        expiry = entry.record.expires_at
        beginning, ending = max(inserted, self.start), min(expiry, end)
        if ending <= beginning:
            return
        row = self._row(node_id, domain)
        row[INDEX["episodes"]] += 1
        row[INDEX["live_seconds"]] += ending - beginning
        row[INDEX["warm_entries" if inserted < self.start else "insertions"]] += 1
        if reason == "evicted":
            row[INDEX["capacity_evictions"]] += 1
        elif expiry <= end:
            row[INDEX["ttl_expirations"]] += 1
            if reason == "snapshot":
                row[INDEX["expired_pending_at_end"]] += 1
        elif reason == "replaced":
            row[INDEX["replacements"]] += 1
        elif reason == "snapshot":
            row[INDEX["live_at_end"]] += 1
        else:
            raise RuntimeError(f"unexpected cache episode exit: {reason}")
        if reason == "expired":
            row[INDEX["expired_removals"]] += 1

    def _install(self):
        original_query = self.layer._query

        def query(node_id, domain, fill, measure, done, *, requester=None, trace=None):
            if node_id not in self.node_ids:
                return original_query(node_id, domain, fill, measure, done, requester=requester, trace=trace)
            if fill and measure:
                self._row(node_id, domain)[INDEX["queries"]] += 1
            previous = self.context
            self.context = (node_id, fill and measure)
            try:
                return original_query(node_id, domain, fill, measure, done, requester=requester, trace=trace)
            finally:
                self.context = previous

        self.layer._query = query
        for node_id in self.node_ids:
            self._wrap_node(node_id)

    def _wrap_node(self, node_id):
        node = self.layer.nodes[node_id]
        original_store, original_remove, original_check = node.on_store, node.on_remove, node.check_cache

        def stored(record):
            now = self.layer.clock.now
            if original_store is not None:
                original_store(record)
            entry = node.cache[record.domain]
            current = self.active[node_id].get(record.domain)
            # Duplicate transfer rejection re-advertises the existing entry.
            if current is not None and current[0] is entry:
                return
            if current is not None:
                raise RuntimeError("new cache entry appeared without a removal event")
            self.active[node_id][record.domain] = (entry, now)

        def removed(record, reason):
            now = self.layer.clock.now
            if original_remove is not None:
                original_remove(record, reason)
            self._close(node_id, record.domain, now, reason)

        def checked(domain, *, foreground=True):
            record = original_check(domain, foreground=foreground)
            if self.context is not None and self.context[0] == node_id:
                if self.context[1] and record is not None:
                    self._row(node_id, domain)[INDEX["hits"]] += 1
            elif node._runtime_measure and foreground:
                row = self._row(node_id, domain)
                row[INDEX["peer_queries"]] += 1
                row[INDEX["peer_hits"]] += int(record is not None)
            return record

        node.on_store, node.on_remove, node.check_cache = stored, removed, checked

    def snapshot(self):
        if self.finished:
            raise RuntimeError("retention snapshot can only be taken once")
        self.finished = True
        end = self.layer.clock.now
        for node_id in self.node_ids:
            for domain in list(self.active[node_id]):
                self._close(node_id, domain, end, "snapshot")
        totals = [sum(row[i] for rows in self.rows.values() for row in rows.values())
                  for i in range(len(FIELDS))]
        if totals[INDEX["episodes"]] != sum(totals[INDEX[field]] for field in (
                "capacity_evictions", "ttl_expirations", "replacements", "live_at_end")):
            raise RuntimeError("retention episode accounting failed")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        suffix = ""
        layout = getattr(self.layer, "layout", None)
        if layout is not None and layout.counts[self.layer.role] > 1:
            suffix = "_shard" + str(layout.workers[self.layer.endpoint]["shard"])
        path = self.output_dir / f"{self.layer.model}_{self.layer.role}{suffix}_retention.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump({
                "fields": FIELDS, "model": self.layer.model, "role": self.layer.role,
                "start": self.start, "end": end, "node_ids": sorted(self.node_ids),
                "rows": {node: [[domain, *row] for domain, row in sorted(rows.items())]
                         for node, rows in sorted(self.rows.items())},
            }, stream, separators=(",", ":"))
        return {"path": str(path), "node_count": len(self.node_ids),
                "start": self.start, "end": end, "totals": dict(zip(FIELDS, totals))}


def attach_observer(layer, options):
    """Observe every forwarder and a fixed, traffic-independent client sample."""
    if layer.role not in ("client", "forwarder"):
        return None
    if layer.role == "forwarder":
        selected = layer.node_ids
    else:
        limit = min(options.get("sample_clients", 300), len(layer.nodes))
        selected = [node for node in layer.node_ids if node < limit]
    return RetentionObserver(layer, selected, options["output_dir"])


def merge_retention(parts, model, role, output_dir):
    """Merge disjoint profile shards after the common clock has frozen."""
    if len(parts) == 1:
        return parts[0]
    rows = {}
    for part in parts:
        with gzip.open(part["path"], "rt", encoding="utf-8") as stream:
            profile = json.load(stream)
        if rows.keys() & profile["rows"].keys():
            raise RuntimeError("retention shards contain duplicate node ownership")
        rows.update(profile["rows"])
    path = Path(output_dir) / f"{model}_{role}_retention.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump({"fields": FIELDS, "model": model, "role": role,
                   "start": parts[0]["start"], "end": parts[0]["end"],
                   "node_ids": sorted(map(int, rows)), "rows": rows}, stream, separators=(",", ":"))
    return {"path": str(path), "node_count": sum(p["node_count"] for p in parts),
            "start": parts[0]["start"], "end": parts[0]["end"],
            "totals": {field: sum(p["totals"][field] for p in parts) for field in FIELDS}}
