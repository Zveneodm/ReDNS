# 集群预取

`prefetch` 分支新增 `ReDNS-prefetch`，保留原 baseline 和无预取 `ReDNS`。

第一次完整实验与取舍见 [实验结果解读](experiments/prefetch/analysis.md)、[结果表](experiments/prefetch/report.md) 和 [当时配置](experiments/prefetch/config.json)。当前配置已改用集群共享的 `budget_ratio` 预算，历史实验不代表当前版本的性能。

后续实现审查、修复及上次数据的逐请求拆解见 [审查报告](experiments/prefetch/audit.md)。第一次完整实验对应提交 `117da42`，不代表后续修复后的性能。

`budget_ratio` 的实现与多集群验证见 [预算验证记录](experiments/prefetch-budget/Readme.md)。

```bash
python3 -m sim.run_comparison --include-prefetch --config config.json --output-dir res/prefetch-budget
python3 -m sim.report_prefetch res/prefetch-budget --output-dir res/prefetch-budget/report
python3 -m sim.run_prefetch --config config.json
```

这是对 [Forestall, Ferreira and Oki, TNSM 2026, Section III-B](https://doi.org/10.1109/TNSM.2026.3704549) 的工程适配，不是作者代码的严格复现。采用请求间隔/TTL 的 Welford 统计、历史窗口 H、建议窗口 A、一次性访问过滤、无效建议过滤和按 deadline 排序。新增集群热度合并、任务限额和按上报负载分配。

每个 forwarder 每 `report_seconds` 模拟秒批量上报其实际收到的前台请求的域名、到达时间、响应绝对期限、权威 TTL，以及本周期处理请求数与当前未完成数。client 本地命中、邻居帮助查询、后台预取均不作为新的需求样本，不读取生成器的偏好或未来序列。不同 forwarder 的间隔摘要不能直接相加，所以上报保留到达时间，manager 按时间合并后计算集群级间隔。报告同时包含最早未完成请求的时间界限；manager 暂存尚无法确定顺序的事件，跨批次有序合并，避免漏计迟到回复。跟踪器拒绝乱序输入。

热度数据按现有 `(cluster × num_domains + domain) % manager_workers` 路由到唯一 manager 分片。每个周期所有成员的报告到齐后合并样本，包括空报告；负载信息送往全部 manager 分片。每个集群独立生成预取列表，不跨集群分享记录或热度。单 forwarder 的集群也启动 manager 执行预取。

manager 在 `advice_seconds` 对应的固定模拟时间边界生成建议，由边界后首个完整报告触发，避免用实际处理时刻重新计时造成周期漂移；错过的历史边界不会集中补跑。H 用于移除最后一次请求已超过窗口的域名，持续活跃域名保留累计统计，间歇超过 H 后重新学习。至少 `alpha` 个间隔才可建议；方差分母为 `样本数 - beta`，不足时跳过。集群适配使用 `平均间隔 + gamma × 标准差` 作为距最后请求的活跃容忍时间；预测下次到达不晚于 `now + A`，且实际观察的有效期不覆盖整个 A，才进入列表。deadline 取预测到达和观察期限的较早者，最早为当前时刻。`epsilon` 跳过连续无需求的建议，`zeta` 移除对应条目；`null` 表示不限制。只有实际完成的预取才计作一次建议，单次建议只结算一次失败；跳过任务不消耗失败额度。这里的 H 与 gamma 处理及按观察期限抑制刷新属于工程选择，不能视为论文 Algorithm 2 的逐行复现。

各域名分片将候选合并到一个集群预算所有者，所有者编号为 `cluster_id % manager_workers`。域名跟踪、区域目录和实际响应处理仍由原来的域名所有者负责。预算所有者按 deadline 排序，同样紧迫的候选优先选择平均请求间隔更短的域名，再按 ID 打破完全相同的排序值。目标 forwarder 按上报前台/帮助请求负载加已派发未完成任务数选择，不再按节点均分每轮配额；负载是周期采样，不能保证调度瞬间的全局最小值。

在 `config.json` 中通过以下字段配置预取预算，其余预取字段可以省略并使用默认值：

```json
"prefetch": {
  "budget_ratio": 0.15,
  "max_inflight_per_forwarder": 8
}
```

`budget_ratio` 为有限的 0–1 数值。每个集群、每个统计阶段，累计收到 N 次来自 client 的前台查询就产生 `floor(N × budget_ratio)` 次预取额度。client 本地命中、forwarder 帮助查询、预取请求和 manager 查询均不产生额度；多个 manager 分片对同一份需求只入账一次。比例 0 表示不派发预取，1 表示最多提供与前台到达量相同的额度。小数精确累计，例如 20 次查询在比例 0.15 下产生 3 次额度。

额度在同一阶段内保留，预热和正式阶段分别记账，正式派发不能使用预热的剩余额度。因此这不是固定的每秒 QPS 上限，也不是候选派发率。每秒持续收到约 4,300 次前台查询时，0.15 对应长期平均约 645 次/秒的预算；积累的未用额度允许之后集中使用。每个任务先预留一次额度，过时、重复或接收方跳过的任务会退还；实际发往权威的预取才最终消耗额度。

额度不足时仍有效的候选留在集群队列，等待后续前台请求提供额度。每次新的建议替换相应域名分片的旧候选；超过原建议窗口的任务会清理。派发时域名所有者还会核对最新的记录期限，已有更新答案的旧建议被取消并退还额度。

`max_inflight_per_forwarder` 独立限制一个 forwarder 同时已派发且尚未完成的预取数量，包括等待执行时刻的任务；完成后可继续派发。执行端也会再次检查实际在途数量。它不再限制一轮的预取总数，单个空闲节点可在一轮内连续处理超过 8 个任务。旧 `max_per_forwarder` 字段兼容映射为这个并发上限，不再代表每轮配额；新旧字段同时配置且数值冲突时会报错。解析后的结果配置使用新字段，便于确认实际生效值。

任务在 deadline 前一个配置权威 RTT 执行，强制访问权威服务器，不因本地旧记录仍有效而短路。响应按普通缓存规则存入、公布区域目录并允许正常逐出转存；TTL 从实际权威回答获得，不修改已有答案的绝对期限。完成结果只更新有效期，不作为新的需求。manager 不保存 DNS 答案。新预取记录可以通过已有区域目录被同集群其他 forwarder 定位和查询。

查询时，`ReDNS-prefetch` 的 manager 将已有有效候选按最近上报负载、编号排序。已有的直属 client 优先路径保持不变；原无预取 `ReDNS` 仍按编号排序。因此实验测量的是预取和相应负载调度的整体效果，不能将全部差异归因于单独的刷新算法。

默认参数：报告周期 1 秒，H=300 秒，A=5 秒，alpha=1、beta=0、gamma=1，epsilon/zeta 不限制，`budget_ratio=0.15`，每个 forwarder 最多 8 个未完成预取。所有参数在 `prefetch` 配置对象中严格校验。计时窗口结束后停止新增建议与预取；已发送的请求、回复和目录更新必须全部排空。

结果保留 `authoritative_queries` 作为前台回源计数，新增 `prefetch_authoritative_queries` 和两者之和 `authoritative_queries_total`。前台 `queries/hits/misses` 不包含后台预取。`forwarder.prefetch_*` 提供任务、跳过、完成、存入与实际使用数量；使用量归属预取接受时的预热/正式阶段，邻居查询也可使其成为已使用。该使用计数跟踪预取落点的缓存条目；逐出后转存到 client 的后续使用计入转存指标，不再计入原条目的预取使用计数。未使用数量不能直接等同浪费，因为实验结束时可能仍有效。

`manager` 总计及 `managers[i]` 每集群统计新增 `prefetch_budget_demand`（已上报并入账的前台请求）、`prefetch_budget_allowance`（累计整数额度）、`prefetch_budget_reserved`（派发预留）、`prefetch_budget_refunded`（退还）和 `prefetch_budget_blocked`（仍有候选但额度不足的报告轮数）。排空后自动检查每集群 `reserved - refunded = 实际预取`，且该数不超过 `floor(demand × budget_ratio)`。末尾尚未上报的前台请求不提供预算，不会因此多发预取。

**主时延 `average_latency_ms` 只累计用户前台查询实际经过的配置 RTT，除以前台请求数。** 热度统计、列表生成、后台调度、预取回源和目录通知的工作不直接计入这个时延，也不增加它的样本数。命中预取记录时仅计前台获取该记录所经过的链路；例如命中直属 forwarder 仍计 client↔forwarder 的 1 ms，但不计此前预取的 110 ms 权威 RTT。`average_wall_latency_ms` 是独立的前台进程/IPC 实耗诊断，可受后台资源竞争影响，不与主时延相加，也不把它作为预取自身耗时收费。上次三组主时延的逐链路核对见 `experiments/prefetch/latency_check.json`，复跑命令为 `python3 experiments/prefetch/check_latency.py`。

三组对照共用积压检查、指纹检查、时间比例、CPU 池和公共进程分配。任一组失败则整轮无效、提高共同比例后全部从空缓存重跑，三份结果同时通过后才发布。正式报告保留在 `experiments/prefetch/`，大结果保留在 Git 忽略的 `res/prefetch/`。
