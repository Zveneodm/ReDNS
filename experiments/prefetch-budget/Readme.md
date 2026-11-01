# budget_ratio 实现验证

当前项目根目录 `config.json` 已显式配置 `prefetch.budget_ratio=0.15` 和 `max_inflight_per_forwarder=8`。前者是每集群、每统计阶段累计的需求预算；后者仅限制未完成任务，不是每轮派发配额。完整机制见 [PREFETCH.md](../../PREFETCH.md)。

域名跟踪和目录仍按原有规则分片；候选合并到 `cluster_id % manager_workers` 对应的预算所有者。只有预算所有者对每份 forwarder 报告入账一次，即使结束时整个报告周期没有收齐，也不遗漏已经收到的需求。候选按照 deadline、平均请求间隔、域名 ID 排序；分配给负载较低且尚有并发余量的 forwarder。

本次完整回归 **178 项通过**，新增 18 项预算测试，覆盖比例 0/1、小数累计、预热/正式隔离、额度退还、过时候选、跨分片只入账一次、集群之间不借额度、帮助请求不提供额度，以及在途上限为 2 时单个节点一轮完成 12 次预取。测试日志见 [tests.log](tests.log)。

额外运行了包含 baseline、ReDNS 和 ReDNS-prefetch 的短多集群对照：300 个 client、10 个 forwarder、2 个集群，2 个 manager 分片，600 请求/模拟秒，预热 4 秒、正式 12 秒、共同 time_scale=1。三组均有 2,404 次预热请求和 7,135 次正式请求，通过相同工作负载/TTL 指纹、CPU 分配、时序门限和流量守恒校验。这个配置用于实现验证，不能与第一次 200+600 秒实验直接比较性能。

预取版正式阶段的预算账本如下：

| 集群 | 已入账前台请求 | floor(请求 × 0.15) | 预留 | 退还 | 实际预取 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 2,102 | 315 | 339 | 26 | 313 |
| 1 | 1,917 | 287 | 308 | 23 | 285 |
| 合计 | 4,019 | 602 | 647 | 49 | 598 |

两集群的整数小数余量各自保留，不互相借用。预留次数可以超过累计额度，因为取消/跳过会归还额度供其他候选使用；`预留 - 退还` 与实际权威预取数一致，始终不超过本集群额度。

结果摘要与实际源代码指纹见 [validation.json](validation.json)，前台逐链路 RTT 账目核对见 [latency_check.json](latency_check.json)。后台预取未计入前台主时延。

复跑短对照：

```bash
python3 -m sim.run_comparison --include-prefetch \
  --config experiments/prefetch-budget/config.json \
  --output-dir res/prefetch-budget-validation
python3 experiments/prefetch/check_latency.py \
  --directory res/prefetch-budget-validation \
  --output res/prefetch-budget-validation/latency_check.json
```

历史 `experiments/prefetch/` 中第一次正式实验的数据保留原样，本次未重跑其完整 200 秒预热和 600 秒正式窗口。
