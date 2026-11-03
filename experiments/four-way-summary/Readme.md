# Four-model historical results under matching standard settings

This report aggregates existing results; it does not rerun the simulator.
30,000 clients; 100 forwarders; cluster size 100; 1,000,000 domains; cache capacity 1,000 at both layers;
15,000 QPS; 200 s warm-up + 600 s formal; time_scale=2; workers=1/1/1; seed=42.
Each model completed 8,998,486 formal requests. The workload and TTL fingerprints match exactly.
Both prefetch models use budget_ratio=0.15. Baseline-prefetch uses local forwarder prediction; ReDNS-prefetch uses cluster prediction.

| Model | Mean configured RTT (ms) | Foreground authority | Prefetch authority | Total authority | Prefetch records used |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | 19.092662 | 1,538,512 | 0 | 1,538,512 | N/A |
| baseline-prefetch | 17.626431 | 1,418,273 | 276,300 | 1,694,573 | 52.76% |
| ReDNS | 13.058173 | 1,020,468 | 0 | 1,020,468 | N/A |
| ReDNS-prefetch | 10.899552 | 838,241 | 392,214 | 1,230,455 | 40.55% |

## Interpretation limits

These are historical runs from different source versions, not a four-way comparison rerun from one code snapshot.
The older baseline/ReDNS results do not include the concurrent-miss coalescing used in the prefetch results.
Differences must not be attributed solely to prefetch. A same-version four-way run would be needed for a strict ablation.
Configured RTT and measured process/IPC latency are separate metrics. Prefetch authority RTT is excluded from foreground RTT samples.
Prefetch record use is the fraction of stored prefetch versions used at least once, not a request hit rate.

## Measured process / IPC overhead

| Model | Mean process/IPC latency (ms) |
| --- | ---: |
| baseline | 0.082133 |
| baseline-prefetch | 2.161408 |
| ReDNS | 1.963672 |
| ReDNS-prefetch | 5.543935 |

## Sources

- baseline: `res/comparison/run-b962cf34023a/baseline_results.json`; source SHA-256 `8680af75f57b4a2d41603b5cc87cec8b97c6a3041c2063e93a04d1a3b147a5ab`.
- baseline-prefetch: `res/20261101-5/baseline-prefetch_results.json`; source SHA-256 `bfda2d80242f4128adb1409c1e57ecad6d941eada911fd6cec54d29593870ccc`.
- ReDNS: `res/comparison/run-b962cf34023a/ReDNS_results.json`; source SHA-256 `8680af75f57b4a2d41603b5cc87cec8b97c6a3041c2063e93a04d1a3b147a5ab`.
- ReDNS-prefetch: `res/prefetch-ablation/run-44d4374192c2/ReDNS-prefetch_results.json`; source SHA-256 `078f115a32c85e7e58209d202607e95c248843200639bac6cc2252c2941d3dc1`.

Excluded the earlier res/prefetch trial and res/ReDNS_prefetch_budget015_results.json because they use time_scale=1 and workers=2/4/2.
Excluded the root standalone baseline_results.json / ReDNS_results.json because they use the older four-layer topology and different workload settings.

The rejected old cluster-placement baseline-prefetch (0.48% use) is not used as the current local-prefetch baseline.

All selected results passed their saved timing gates; raw request counts, mean RTT and authority flow were checked. See summary.json for exact values and fingerprints.
