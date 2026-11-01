"""The only two experiment models and their shared authoritative server."""

from zlib import crc32

from src.cache import DNSCache
from src.cooperation import CopyDirectory, spare_target
from src.records import Record


class Client(DNSCache):
    """A client's private cache; its fixed forwarder is wired by the simulator."""


class BaselineForwarder(DNSCache):
    """Ordinary cache: every local miss goes directly to the authority."""


class ReDNSForwarder(DNSCache):
    """A forwarder with a directory of its directly connected clients."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.copy_directory = CopyDirectory()

    def peer_target(self, domain, *, exclude=()):
        return self.copy_directory.choose(domain, self.clock.now, exclude=exclude)

    def region_deadline(self, domain):
        """Merge own cache and direct-client hints into one availability TTL."""
        entry = self.cache.get(domain)
        own = (entry.record.expires_at if entry is not None
               and not entry.record.is_expired(self.clock.now) else 0)
        return max(own, self.copy_directory.latest_deadline(domain, self.clock.now))

    def transfer_target(self, record, clients, client_capacity):
        """Skip known copies; choose a spare client slot by stable hashing."""
        if record.is_expired(self.clock.now) or not clients or not client_capacity:
            return None
        if self.peer_target(record.domain) is not None:
            self.stats["transfer_skipped_known_copies"] += 1
            return None
        name_hash = crc32(f"domain-{record.domain:05d}.example".encode())
        target = spare_target(clients, self.copy_directory.child_counts, client_capacity, name_hash)
        if target is None:
            self.stats["transfer_skipped_full_target"] += 1
        return target

    def metrics(self):
        return {**super().metrics(), "peer_directory_entries": self.copy_directory.entries}


class AuthoritativeServer:
    """Fixed per-domain TTLs; each lookup returns a fresh answer lifetime."""

    def __init__(self, ttls, clock):
        self.ttls = tuple(ttls)
        self.clock = clock
        self.queries = 0

    def lookup(self, domain, *, measure=True):
        if measure:
            self.queries += 1
        ttl = self.ttls[domain]
        return Record(domain, self.clock.now + ttl, ttl)
