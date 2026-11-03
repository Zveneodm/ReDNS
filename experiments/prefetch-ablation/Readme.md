# baseline + prefetch 与 ReDNS + prefetch

此目录保存旧版本标准实验：baseline 当时也使用集群汇总预测和低负载派发。0.48% 使用比例属于那个版本。当前 baseline 已改为每个 forwarder 独立预测、固定在本地预取，保留相同的集群共享预算比例；新实现与验证见 [本地预取基线](../local-prefetch/Readme.md)。本目录的历史结果没有重写为新算法结果。

该实验在相同的集群预取策略下，对比普通 DNS 缓存与 ReDNS 协作缓存，衡量 ReDNS 对用户时延和权威查询成本的额外作用。两组都启用并发 miss 合并。

**标准实验已完成**：每组 8,998,486 次正式请求，平均配置 RTT 降低 43.63%，含预取的总回源降低 35.64%。完整数据、机制解释和实际 IPC 开销见 [结果分析](analysis.md)，自动生成的结果表见 [report.md](report.md)。

| 机制 | baseline-prefetch | ReDNS-prefetch |
| --- | --- | --- |
| client / forwarder 普通 TTL 缓存 | 开启 | 开启 |
| 集群热度上报与 Forestall 跟踪器 | 相同 | 相同 |
| 集群预取预算、低负载派发、并发上限 | 相同 | 相同 |
| 预取完成后的缓存替换规则 | 普通 LRU | 普通 LRU |
| 直属 client 副本目录与取回 | 关闭 | 开启 |
| 区域可用性发布与邻居取回 | 关闭 | 开启 |
| forwarder 逐出后的空位转存 | 关闭 | 开启 |

baseline-prefetch 的 manager 只参与预取，不提供查询答案或发现其他 forwarder 的副本。预取记录只能在落点 forwarder 被访问；ReDNS-prefetch 可通过既有目录与协作查询让组内其他 forwarder 使用它。这是对现有集群预取策略增加 ReDNS 整套协作机制的消融，不代表所有按独立 resolver 定制的预取方法。

## 运行

```bash
python3 -m sim.run_comparison --prefetch-ablation --config experiments/prefetch-ablation/config.json --output-dir res/prefetch-ablation
python3 -m sim.report_prefetch res/prefetch-ablation --output-dir experiments/prefetch-ablation
python3 experiments/prefetch/check_latency.py --directory res/prefetch-ablation --output experiments/prefetch-ablation/latency_check.json
python3 experiments/prefetch-ablation/verify.py
```

单独运行新增模型：

```bash
python3 -m sim.run_baseline_prefetch --config config.json
```

`config.json` 是启动实验时项目根目录标准配置的原样快照：30,000 client，100 forwarder，单集群，1,000,000 域名，client / forwarder 各 1,000 条缓存，15,000 QPS，预热 200 秒、正式 600 秒、time_scale=2，seed=42。各角色按默认 1 个 worker，两个预取模型都有 manager。`budget_ratio=0.15`，每 forwarder 最多 8 个未完成预取。请求、TTL、容量、替换策略、CPU 和共同时间比例由对照框架核对；任一组时序不合格，则两组一起重跑，不拼接旧结果。

预算依赖各模型实际到达 forwarder 的前台查询，相同比例不意味着预取总数相同。因此报告同时展示前台时延、前台回源、后台预取回源和总回源；不会仅凭前台 miss 下降忽略额外流量。预取使用比例跟踪正式存入的落点记录是否至少使用一次，不追踪逐出并转存后的完整因果链，观察末尾未使用也不等于浪费。

主时延为前台请求经过的配置 RTT，预取工作不直接收费；进程和 IPC 实耗另列。模拟器不按配置 RTT 休眠，因此并发合并率取决于实际解析何时完成。所有效果结论只适用于本次合成负载、参数和单个 seed；结果由正式实验数据决定。

实现回归：196 项测试通过，覆盖新模型的隔离、相同预取派发、落点命中与跨节点发现差异、并发合并、真实分片进程、两组发布及 manager CPU 一致性、报告生成。本轮 `report.md`、`summary.json`、`verification.json`、`latency_check.json` 和 `diagnostics.json` 已保存。配置、流量守恒、逐请求回源标记与前台 RTT 账本全部核对通过；后台预取 RTT 未计入前台时延。逐请求大文件保留在 Git 忽略的 `res/prefetch-ablation/run-44d4374192c2/`。
