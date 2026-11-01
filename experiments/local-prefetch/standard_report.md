# Baseline with per-forwarder prefetch: standard experiment

Result: `baseline-prefetch_results.json`; scope: per forwarder; budget ratio: 15%.
Warm-up: 200 s; formal window: 600 s; time_scale=2.0.

| Metric | Value |
| --- | ---: |
| Formal requests | 8,998,486 |
| Mean configured RTT (ms) | 17.626431 |
| p95 configured RTT (ms) | 111.000 |
| Foreground authority queries | 1,418,273 |
| Background prefetch queries | 276,300 |
| Total authority queries | 1,694,573 |
| Prefetch records stored | 276,300 |
| Prefetch records used at least once | 145,779 |
| Prefetch use fraction | 52.7611% |
| Prefetch / reported forwarder demand | 10.6975% |
| Mean process/IPC latency (ms) | 2.161408 |

The configuration, code fingerprint, request flow, shared quota and foreground RTT ledger passed verification.
Prefetch authority RTT is excluded from foreground samples. Process/IPC latency is a separate diagnostic.
This is a standalone single-seed run; no matched ReDNS rerun is included.
