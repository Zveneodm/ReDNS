# baseline 的本地预取与共享预算

baseline-prefetch 现改为每个 forwarder 独立运行 Forestall，修复旧基线把集群热度对应的预取记录派到无关节点的问题。ReDNS-prefetch 的集群预测、低负载派发和共享查询逻辑保留。

## 预测和落点

本地实现位于 `src/auxiliary/local_prefetch.py`，使用同一个 Forestall 类及参数：

- 每个 forwarder 自己保存热度、请求间隔、观察到的有效期和建议历史。
- 按自己的前台请求到达时间排序，只消费已越过本地未完成请求水位的回复，避免异步回复造成乱序。
- 自己运行算法生成列表，任务固定在提出候选的 forwarder 执行。空闲的其他节点不能替代它。
- 同一域名在不同 forwarder 的候选、在途状态和有效期分别管理，一台刷新不会抑制另一台的预测。
- manager 只接收需求计数与已生成的候选，发放共享额度；不汇总原始热度样本、不运行 baseline 的 Forestall。

授权任务执行前会再次检查本地有效期与候选窗口；正常查询已取回更新答案、窗口结束、零容量等跳过任务退还额度。预取不计为新的需求，前台查询也不等待正在执行的后台预取。

## 总预算与 ReDNS 保持同一口径

两组使用相同的 `prefetch.budget_ratio`，当前仍为 **0.15**。每个集群、每个阶段的额度均为：

```text
allowance = floor(该集群累计上报的 forwarder 前台请求数 × budget_ratio)
actual_prefetch = reserved - refunded <= allowance
```

不在每个 forwarder 单独取整，因此 20 个节点各收到一次请求，比例 0.15 时共享额度是 3 次，而不是 0 次。预热和正式预算分别记账；client 本地命中、帮助请求及后台预取不产生额度。`max_inflight_per_forwarder=8` 仍限制实际未完成任务数，不限制一轮总次数。

这里保证的是相同的预算比例和分母口径，**不强制两组实际执行次数完全相同**。本地与集群算法的候选数、实际到达 forwarder 的请求数都会不同；没有候选时不填充无意义任务凑次数。报告保留预算需求、额度和实际预取占比，供比较实际流量成本。

## 运行

根配置的 `prefetch.enabled=true` 时，普通对照入口自动比较新版本地 baseline-prefetch 与 ReDNS-prefetch：

```bash
python3 -m sim.run_comparison --config config.json
```

也可显式使用 `--prefetch-ablation`。结果仍默认保存为 `res/YYYYMMDD-N/`。结果新增 `prefetch_scope=forwarder/cluster`、`prefetch_budget_scope=cluster` 和 `prefetch_budget_basis=forwarder_foreground_queries`，明确预测与预算范围；baseline 的 `forwarder.prefetch_local_*` 记录本地观察与算法运行次数，manager 的预测计数为零。

旧 `experiments/prefetch-ablation/` 中的 0.48% 属于旧集群派发基线，保留为历史数据，不能作为新版性能结果。新报告能识别旧结果并标明其策略。

## 验证

211 项回归检查通过，新增覆盖本地热度/TTL 隔离、固定落点、同域名多节点同时预取、忙节点等待、共享小数额度、跨进程及 manager 分片、乱序回复、过时任务退还和零比例/零容量。报告也检查了旧预算字段缺失时的读取兼容。

迁移前后，用固定时钟比较 ReDNS、ReDNS-prefetch 与无预取 baseline，覆盖 LRU/random 共 6 组；忽略新加的零值观测计数后，答案与全部原有指标完全相同。只改变 baseline-prefetch 的预测和落点行为。

另做了 300 client、6 forwarder、2 集群、2/2/2 个 client/forwarder/manager worker、1,500 QPS、预热 3 秒及正式 12 秒的短验证。两组各 17,897 次正式请求，均通过时序、流量及预算校验，输入指纹一致：

| 模型 | 预测范围 | 已上报预算需求 | 额度 | 实际预取 | manager 预测次数 |
| --- | --- | ---: | ---: | ---: | ---: |
| baseline-prefetch | forwarder | 7,719 | 1,157 | 763 | 0 |
| ReDNS-prefetch | cluster | 8,107 | 1,215 | 914 | 8 |

两组配置比例均为 0.15。baseline 的 6 个节点各自执行了本地算法，正式阶段共 12 次预测运行；manager 不做预测。逐请求 RTT 与前台链路账本一致，排除后台预取费用。本轮用于检验实现，不是原标准 200+600 秒实验的重跑，不据此更新完整性能结论。

短验证配置、结果表、机器可读汇总与检查保存在本目录；原始结果位于 `res/20261101-4/`。

## 后续标准配置单独运行

本地预取 baseline 随后按根目录标准配置完成了 200 秒预热和 600 秒正式窗口，time_scale=2，各处理节点和参数均未调整。正式请求为 8,998,486 次，平均配置 RTT 为 17.626431 ms；前台权威查询 1,418,273 次，后台预取 276,300 次，总权威查询 1,694,573 次。

276,300 个正式预取版本中，145,779 个至少使用一次，使用比例为 52.76%。实际预取占已上报 forwarder 前台需求的 10.70%，低于 15% 预算上限。源码与启动配置、时序、流量守恒和前台 RTT 账本均核对通过，后台预取 RTT 仍排除在前台时延之外。

该次完整运行的小型报告、汇总和验证记录归档为 [standard_report.md](standard_report.md)、[standard_summary.json](standard_summary.json) 和 [standard_verification.json](standard_verification.json)，大结果仍位于 `res/20261101-5/`。相同配置和输入指纹的四组历史数据汇总见 [four-way-summary](../four-way-summary/Readme.md)；其中包含较早版本结果，不是同一代码版本的四组重跑。
