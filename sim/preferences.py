"""Hierarchical request preferences and bounded online downstream learning."""

from array import array
from bisect import bisect_right
from collections import defaultdict
from math import exp, fsum, gcd, log
import random


def _cdf(size, alpha):
    values = array("d")
    total = 0.0
    for rank in range(1, size + 1):
        total += rank ** -alpha
        values.append(total)
    return values


def _coprime_multiplier(value, modulus):
    if modulus <= 1:
        return 0
    value = value % modulus or 1
    while gcd(value, modulus) != 1:
        value += 1
    return value


class HierarchicalZipf:
    """Exact per-client Zipf ranks with region-biased independent rankings.

    A client's leading ranks are sampled without replacement from its region's
    Zipf prior.  Remaining ranks form a deterministic bijection over all other
    domains, so every client still has a full, duplicate-free ranking.
    """

    def __init__(self, config):
        options = config["regional_preferences"]
        self.size = config["num_domains"]
        self.regions = config["num_forwarders"]
        self.seed = config["seed"]
        self.head_size = min(options["shared_rank_count"], self.size)
        self.client_cdf = _cdf(self.size, options["client_zipf_alpha"])
        self.region_cdf = _cdf(self.size, options["region_zipf_alpha"])
        self._heads = {}
        self._region_maps = []
        for region in range(self.regions):
            rng = random.Random(self.seed + 0x6A09E667 + region * 0x9E3779B1)
            a = _coprime_multiplier(rng.randrange(1, self.size + 1), self.size)
            self._region_maps.append((a, rng.randrange(self.size)))

    def sample_rank(self, rng):
        return min(bisect_right(self.client_cdf, rng.random() * self.client_cdf[-1]),
                   self.size - 1)

    def _head(self, client):
        cached = self._heads.get(client)
        if cached is not None:
            return cached
        rng = random.Random(self.seed + 0xBB67AE85 + client * 0x85EBCA77)
        chosen, seen = [], set()
        attempts = 0
        while len(chosen) < self.head_size and attempts < max(100, 100 * self.head_size):
            attempts += 1
            rank = min(bisect_right(self.region_cdf, rng.random() * self.region_cdf[-1]),
                       self.size - 1)
            if rank not in seen:
                seen.add(rank)
                chosen.append(rank)
        # Extremely large alphas can make all but a few CDF buckets smaller
        # than floating-point resolution.  Complete the bijection rather than
        # allowing rejection sampling to stall.
        if len(chosen) < self.head_size:
            for rank in range(self.size):
                if rank not in seen:
                    chosen.append(rank)
                    if len(chosen) == self.head_size:
                        break
        tail_size = self.size - self.head_size
        if tail_size:
            tail_rng = random.Random(self.seed + 0x3C6EF372 + client * 0xC2B2AE3D)
            tail_a = _coprime_multiplier(tail_rng.randrange(1, tail_size + 1), tail_size)
            tail_b = tail_rng.randrange(tail_size)
        else:
            tail_a = tail_b = 0
        cached = (array("I", chosen), array("I", sorted(chosen)), tail_a, tail_b)
        self._heads[client] = cached
        return cached

    def prepare(self, clients):
        """Build rankings before the real-time clock starts."""
        for client in range(clients):
            self._head(client)

    @staticmethod
    def _unrank_excluding(index, excluded):
        """Return the index-th non-excluded non-negative integer."""
        value = index
        while True:
            skipped = bisect_right(excluded, value)
            candidate = index + skipped
            if candidate == value:
                return value
            value = candidate

    def domain_for_rank(self, client, rank):
        head, excluded, tail_a, tail_b = self._head(client)
        if rank < self.head_size:
            regional_rank = head[rank]
        else:
            tail_size = self.size - self.head_size
            if not tail_size:
                regional_rank = head[rank]
            else:
                tail_rank = (tail_a * (rank - self.head_size) + tail_b) % tail_size
                regional_rank = self._unrank_excluding(tail_rank, excluded)
        a, b = self._region_maps[client % self.regions]
        return (a * regional_rank + b) % self.size if self.size > 1 else 0

    def sample(self, client, rng):
        return self.domain_for_rank(client, self.sample_rank(rng))


class PreferenceLearner:
    """Bounded, decayed empirical model built only from forwarded requests."""

    def __init__(self, half_life, max_domains):
        self.half_life = float(half_life)
        self.max_domains = max_domains
        self.rows = {}
        self.by_domain = defaultdict(set)
        self.fit_state = {}
        self.observations = 0

    def _decay(self, weight, elapsed):
        if elapsed <= 0 or self.half_life <= 0:
            return weight
        return weight * exp(-0.6931471805599453 * elapsed / self.half_life)

    def observe(self, child, domain, now):
        row = self.rows.setdefault(child, {})
        previous = row.get(domain)
        weight = 1.0 if previous is None else self._decay(previous[0], now - previous[1]) + 1.0
        known_expiry = 0.0 if previous is None else previous[2]
        fitted_rate = 0.0 if previous is None else previous[3]
        row[domain] = (weight, now, known_expiry, fitted_rate)
        last_fit, dirty = self.fit_state.get(child, (-float("inf"), 0))
        self.fit_state[child] = (last_fit, dirty + 1)
        self.by_domain[domain].add(child)
        self.observations += 1
        if len(row) > self.max_domains:
            victim = min(row, key=lambda key: self._decay(row[key][0], now - row[key][1]))
            del row[victim]
            children = self.by_domain[victim]
            children.discard(child)
            if not children:
                del self.by_domain[victim]

    def note_copy(self, child, domain, expires_at):
        row = self.rows.get(child)
        if row is not None and domain in row:
            weight, updated, _, fitted_rate = row[domain]
            row[domain] = (weight, updated, expires_at, fitted_rate)

    def forget_copy(self, child, domain, removed_expiry):
        row = self.rows.get(child)
        if row is not None and domain in row:
            weight, updated, known, fitted_rate = row[domain]
            if known and known <= removed_expiry + 1e-9:
                row[domain] = (weight, updated, 0.0, fitted_rate)

    def _refit(self, child, now):
        row = self.rows[child]
        ranked = sorted(row, key=lambda domain: self._decay(
            row[domain][0], now - row[domain][1]), reverse=True)
        if len(ranked) < 3:
            self.fit_state[child] = (now, 0)
            return
        weights = [self._decay(row[d][0], now - row[d][1]) for d in ranked]
        xs = [log(i) for i in range(1, len(ranked) + 1)]
        ys = [log(max(weight, 1e-300)) for weight in weights]
        xbar, ybar = fsum(xs) / len(xs), fsum(ys) / len(ys)
        xx = fsum((x - xbar) ** 2 for x in xs)
        slope = (fsum((x - xbar) * (y - ybar) for x, y in zip(xs, ys)) / xx
                 if xx else 0.0)
        alpha = max(0.0, -slope)
        normalization = fsum(rank ** -alpha for rank in range(1, len(ranked) + 1))
        total_rate = 0.6931471805599453 * fsum(weights) / max(self.half_life, 1e-12)
        for rank, domain in enumerate(ranked, 1):
            weight, updated, expiry, _ = row[domain]
            fitted = total_rate * rank ** -alpha / normalization
            row[domain] = (weight, updated, expiry, fitted)
        self.fit_state[child] = (now, 0)

    def choose(self, domain, allowed, remaining_ttl, now):
        candidates = self.by_domain.get(domain)
        if not candidates:
            return None, 0.0
        best_child, best_score = None, -1.0
        for child in candidates:
            if child not in allowed:
                continue
            last_fit, dirty = self.fit_state.get(child, (-float("inf"), 0))
            if dirty >= 16 or now - last_fit >= min(60.0, self.half_life):
                self._refit(child, now)
            weight, updated, known_expiry, fitted_rate = self.rows[child][domain]
            if known_expiry > now:
                continue
            weight = self._decay(weight, now - updated)
            # Decayed observations per half-life are a stable online rate
            # estimate.  Convert it into probability of reuse before expiry.
            empirical_rate = (0.6931471805599453 * weight /
                              max(self.half_life, 1e-12))
            rate = (empirical_rate + fitted_rate) * 0.5 if fitted_rate else empirical_rate
            score = 1.0 - exp(-rate * max(0.0, remaining_ttl))
            if score > best_score or (score == best_score and child < best_child):
                best_child, best_score = child, score
        return best_child, max(0.0, best_score)

    def metrics(self, now):
        alphas, r2s = [], []
        for row in self.rows.values():
            counts = sorted((self._decay(v, now - t) for v, t, _, _ in row.values()),
                            reverse=True)
            if len(counts) < 3 or counts[-1] <= 0:
                continue
            xs = [log(i) for i in range(1, len(counts) + 1)]
            ys = [log(value) for value in counts]
            xbar, ybar = fsum(xs) / len(xs), fsum(ys) / len(ys)
            xx = fsum((x - xbar) ** 2 for x in xs)
            if not xx:
                continue
            slope = fsum((x - xbar) * (y - ybar) for x, y in zip(xs, ys)) / xx
            predicted = [ybar + slope * (x - xbar) for x in xs]
            total = fsum((y - ybar) ** 2 for y in ys)
            residual = fsum((y - p) ** 2 for y, p in zip(ys, predicted))
            alphas.append(-slope)
            r2s.append(1.0 - residual / total if total else 0.0)
        return {
            "observed_requests": self.observations,
            "tracked_children": len(self.rows),
            "tracked_child_domain_pairs": sum(map(len, self.rows.values())),
            "fitted_zipf_alpha_mean": fsum(alphas) / len(alphas) if alphas else 0.0,
            "fitted_zipf_r2_mean": fsum(r2s) / len(r2s) if r2s else 0.0,
            "fit_scope": "forwarded_cache_misses",
        }

