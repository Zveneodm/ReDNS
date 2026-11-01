# ReDNS

`prefetch` 分支新增集群预取模型及三组正式对照，机制、参数与运行方法见 [PREFETCH.md](PREFETCH.md)。原有两模型入口继续保留。

当前 `config.json` 的 `prefetch.budget_ratio=0.15` 表示每个集群累计每 100 次到达 forwarder 的前台查询提供 15 次预取额度。`max_inflight_per_forwarder=8` 只限制同时未完成的任务，每轮预取次数不再固定限制为 8。

ReDNS 是一个使用 Python 标准库实现的 DNS 缓存模拟项目，提供三种模型：

- **baseline**：client 和 forwarder 各自维护普通缓存，forwarder 未命中就查询权威服务器。
- **ReDNS**：forwarder 未命中后先查询直属 client 副本，再向本集群 manager 定位可用区域，并定向查询相应 forwarder；没有可用区域或已知候选均失败后查询权威服务器。
- **ReDNS-prefetch**：在 ReDNS 上增加集群热度上报、manager 生成预取列表、低负载 forwarder 执行预取，以及按上报负载排序区域查询候选。用 `--include-prefetch` 启用三组对照。

实验拓扑为 **client → forwarder → 权威服务器**，同集群 forwarder 可以互相查询缓存。每个 client 固定连接一个 forwarder，编号映射为 `client_id % num_forwarders`；副本目录只在所属 forwarder 内维护，集群外不相互查询。

## 项目框架

```text
ReDNS/
├── src/                         # 模型与协议，不依赖进程调度
│   ├── records.py               # 不可变 DNS 记录、绝对到期时间
│   ├── cache.py                 # 两组共用的 TTL 缓存、转存接收和统计
│   ├── replacement.py           # LRU / 等概率随机替换
│   ├── expiry.py                # 带版本的过期最小堆
│   ├── cooperation.py           # 直属 client 副本目录、空位选择
│   ├── cluster.py               # 连续编号分组与集群查询边界
│   ├── manager.py               # 每域名、每区域一个合并可用性条目
│   ├── models.py                # Client、两种 Forwarder、权威服务器
│   └── protocol.py              # 查询、回复、目录通知、转存协议
├── sim/                         # 模拟实验运行代码
│   ├── config.py                # 配置读取与严格校验
│   ├── arrivals.py              # 泊松到达和 client / 域名抽样
│   ├── preferences.py           # 区域与 client 的合成 Zipf 需求
│   ├── clock.py                 # 多进程单调时钟；单测使用的手动时钟
│   ├── realtime.py              # 缓存及 manager 进程、CPU 绑定、消息队列
│   ├── parallel.py              # 分片所有权、消息路由及统计合并
│   ├── timing.py                # 到达滞后、持续积压和运行超时检查
│   ├── comparison.py            # 共同速率试跑、成对重试与一致性校验
│   ├── metrics.py               # 聚合统计与流量守恒校验
│   ├── analyze_origin.py        # 从已保存结果重放回源请求历史及理想缓存参考
│   ├── retention.py             # 可选域名级缓存留存观察，不改变缓存规则
│   ├── observe_retention.py     # 留存观察、热度分组与报告入口
│   ├── experiment.py            # 命令行、结果文件写入
│   ├── progress.py              # 终端进度
│   ├── run_baseline.py          # baseline 入口
│   ├── run_comparison.py        # 正式成对对照入口
│   └── run_ReDNS.py             # ReDNS 入口
├── tests/                       # 缓存、协议及真实多进程回归检查
├── config.json                  # 运行时配置
├── res/                         # 实验结果，不将大结果文件纳入 Git
├── Readme.md                    # 项目说明与运行方法
└── ReDNS_ARCHITECTURE.md         # 与当前代码对应的详细机制说明
```

原有 `data/` 样本保留；当前实验使用合成请求，不读取其中的 Excel 文件。

## 运行

需要 Python 3.9+，无需第三方依赖。建议在 Linux / WSL 下运行，以使用 CPU 绑定。

在项目根目录执行：

```bash
python3 -m sim.run_comparison
python3 -m unittest discover -s tests -v
```

也支持直接执行脚本、指定配置或输出位置：

```bash
python3 sim/run_comparison.py --config config.json --output-dir res/comparison
python3 sim/run_baseline.py --config config.json --output res/baseline_results.json
python3 sim/run_ReDNS.py --config config.json --output res/ReDNS_results.json
```

进程数通过 `workers` 配置；`config.json` 省略该项，各角色默认 1 个进程。预取正式对照配置 `experiments/prefetch/config.json` 使用 2 个 client、4 个 forwarder、2 个 manager，加权威和请求调度各 1 个，共 10 个模拟进程。baseline 使用相同的公共进程及其 CPU，manager 槽位空闲。父进程协调运行与收集结果，不计入这些模拟进程。Linux / WSL 下可绑定 CPU；可用 CPU 不足时复用，client / forwarder 进程数不会超过各自节点数。

client 和 forwarder 按 `节点 ID % 进程数` 分片，各节点缓存只有一个所有者。manager 按 `(集群 ID × 域名总数 + 域名 ID) % manager进程数` 分片，同一域名的更新、查询和失效反馈进入同一所有者；即使全部 forwarder 组成一个集群，也可以使用多个 manager 核。逻辑上的每集群一个 manager 及其查询范围保持不变。

多个 client 进程时，由独立调度进程生成唯一的全局请求序列、控制全局 `max_in_flight` 并收集按请求顺序保存的延迟；调度消息不增加配置 DNS RTT，但真实 IPC 耗时计入实际进程请求耗时。所有进程共用同一起点、缩放比例和排空检查。若三个 `workers` 数都设为 1，则兼容原来的三进程 / 四进程运行，client 进程兼任调度。

## 两组模型的机制

两组共用同一套 TTL 缓存和替换策略。查询先检查 client 缓存，再检查对应 forwarder 缓存。

| forwarder 本地未命中后 | baseline | ReDNS |
| --- | --- | --- |
| 查询下游目录 | 无 | 查本 forwarder 的目录 |
| 有有效副本提示 | 仍查权威 | 按编号查询目录中的 client，仅查其本地缓存 |
| 下游副本查询失败 | 不适用 | 删除失效提示，尝试剩余提示 |
| 本地 client 均无可用副本 | 查权威 | 查询本集群 manager，定位可用区域 |
| manager 无其他区域的有效提示 | 不适用 | 直接查询权威，不扫描邻居 |
| 邻居收到集群请求 | 不适用 | 先检查自身缓存，再查询自己的 client 目录 |
| manager 返回的候选均失败 | 不适用 | 反馈失效版本，由原请求 forwarder 查询权威 |
| 目录维护 | 无 | client 实际存入、替换、淘汰或发现过期时通知 |
| forwarder 淘汰有效记录 | 丢弃 | 已有下游副本则跳过，否则尝试转存到空位 |

ReDNS 的后台转存只使用 client 空余容量，接收端会再次检查容量；不挤占有效记录，不主动刷新、不延长 TTL。普通查询的响应回填仍按正常缓存规则替换记录。client 是末级，不能继续转存。

副本目录只保存“域名 ID → client ID → 到期时间”。它不保存答案、不读取其他进程的缓存，也不使用请求生成器的真实偏好。目录提示可能滞后，必须通过实际查询确认。

`forwarder_cluster_size=3`、`num_forwarders=30` 时，编号 0–2、3–5、……、27–29 各组成一个集群，组内互为邻居。每组一个逻辑 manager，只返回本组中有有效区域提示的 forwarder，排除请求方自身。候选按 ID 排序，定向查询命中即停止；每个目标先查自己的缓存，再按目录查询直属 client。帮助请求不再转询其他 forwarder，也不代查权威。末组可以不足配置大小；大小超过总数时全部一组，大小为 1 时关闭集群协作。

manager 只保存 `域名 → {forwarder ID: (区域到期时间, 版本号)}`。区域到期时间取该 forwarder 自身缓存及其已知直属 client 副本中的最大有效期限，不区分副本位于哪一层，不存 DNS 答案，也不持有 client 级目录。自身缓存逐出但 client 仍有副本时，区域条目会保留；最后一份已知副本消失才撤销。区域有效期限改变时通过消息更新，未变化时不重复发布；查询和迟到失败反馈会按到期时间、版本校验。

邻居 client 命中会正常回填帮助方、原请求 forwarder 及请求 client，保持原绝对到期时间。后台转存仍只发给本 forwarder 的直属 client。邻居 RPC、manager 查询和目录更新都通过消息队列路由到目标所有者；即使两个节点恰好属于同一进程，也经过队列，不直接跨节点读取缓存。目录可能暂时漏报或滞后；目标会检查真实缓存，失效提示不会直接算作命中。

旧版偏好学习和 TTL 补足模型已移除；新增主动预取只在 `ReDNS-prefetch` 中启用。

## 配置

完整默认值和校验见 [sim/config.py](sim/config.py)。未知字段和无效值会直接报错。

当前 `config.json` 为 30,000 个 client、100 个 forwarder、全局平均 15,000 请求/模拟秒、预热 200 秒、正式窗口 600 秒。每个 client 与 forwarder 的缓存容量均为 1,000 条，域名总数为 1,000,000。

当前集群大小为 100，形成 1 个集群和对应的逻辑 manager。省略 `forwarder_cluster_size` 时默认 1；baseline 忽略集群协作设置。预取实现及回归测试也覆盖多个独立集群和单节点集群。

| 配置项 | 含义 |
| --- | --- |
| `seed` | 控制到达、请求选择、权威 TTL 抽样和缓存随机替换的独立随机流 |
| `warm_up_seconds` / `duration_seconds` | 预热及正式到达窗口，整数秒 |
| `queries_per_second` | 所有 client 合计的泊松到达强度，允许 0 |
| `num_clients` / `num_forwarders` | 节点数量 |
| `forwarder_cluster_size` | 每个集群的 forwarder 数量，正整数；1 关闭集群协作，当前 100 |
| `client_cache_size` / `forwarder_cache_size` | 每个节点的缓存条数，0 表示关闭 |
| `replacement_policy` | `lru` 或 `random` |
| `num_domains` | 合成域名数量，内部以整数 ID 表示 |
| `ttl_distribution` | `values` 与 `probabilities` 指定权威 TTL 分布 |
| `ttl_range` | 未设置离散 TTL 分布时的均匀整数范围 |
| `query_distribution` / `zipf_alpha` | 全局 `uniform` 或 `zipf` 需求 |
| `regional_preferences` | 合成区域/client 需求，仅含 `enabled`、`region_zipf_alpha`、`client_zipf_alpha`、`shared_rank_count` |
| `client_forwarder_RTT` | client ↔ forwarder 往返延迟，当前 1 ms |
| `forwarder_forwarder_RTT` | 集群内 forwarder ↔ forwarder 往返延迟，当前 2 ms |
| `forwarder_manager_RTT` | forwarder ↔ 本集群 manager 查询往返延迟，当前 1 ms |
| `forwarder_authoritative_RTT` | forwarder ↔ 权威服务器往返延迟，当前 110 ms |
| `time_scale` | 每模拟秒占用的真实秒数，当前 2；成对入口也接受 `auto`，两组共用一个比例 |
| `min_time_scale` / `calibration_requests` | 共同试跑比例下限、试跑正式窗口的最小期望请求数（不超过原正式窗口） |
| `timing` | 积压判定阈值、共同试跑长度和重试参数，见下文 |
| `max_in_flight` | 未完成的客户端请求上限；积压请求保留，不丢弃 |
| `pin_cpu` | 是否绑定逻辑 CPU |
| `workers` | client / forwarder / manager 进程数；省略各项时默认 1 |
| `record_wall_latencies` | 是否保存逐请求进程往返耗时；关闭后仍有均值和最大值 |
| `output_dir` | 结果目录，相对项目根目录 |

直连权威 RTT 初始设为 110 ms，沿用重构前配置中两段上游 RTT 之和（10 + 100 ms），可单独修改。旧的 public DNS 容量和两段 RTT 配置已删除。

默认配置路径及 `output_dir` 相对项目根目录；显式 `--config` 和 `--output` 的相对路径按当前工作目录解释。导入模块不会启动实验。

## 时钟、延迟与对照方式

TTL 按所有进程共用起点的缩放单调时钟计算；响应跨进程传递绝对到期时间。预热保留缓存，正式统计按请求的计划到达时间区分，跨窗口回复和转存继续沿用原请求的阶段标记。

`average_latency_ms` 累计实际链路的配置 RTT，不通过 sleep 等待。没有前序失败时，当前配置对应 client 命中 0 ms、forwarder 命中 1 ms、直属 client 命中 2 ms、manager 定位后邻居缓存命中 4 ms、邻居 client 命中 5 ms。manager 查不到候选后走权威为 112 ms；集群大小为 1、不问 manager 时直达权威为 111 ms。每次实际 client 探测加 1 ms，每次目标 forwarder 探测加 2 ms；失败也收费。无候选时不探测邻居，目录中有多条失效候选时依次计算实际探测成本。

`average_wall_latency_ms` 单独记录真实进程执行、排队、IPC 和调度耗时，不包含额外等待的配置 RTT。目录通知和转存经过真实消息队列，但尚未模拟它们的网络带宽和传输等待，因此累计 RTT 不能直接视为部署后的完整响应时间。

正式对照使用统一入口，例如：

```bash
python3 -m sim.run_comparison --time-scale auto
```

入口先以共同的 `time_scale` 分别运行两组完整协议试跑，包含正常缓存回填、逐出、目录通知和 manager 路径。通过后，两组都重新创建进程、从空缓存运行相同长度的预热和正式窗口。数值 `time_scale` 是首次尝试的比例，`auto` 从 `timing.auto_initial_time_scale`（默认 2）开始；两者均由同一条成对流程验证。

整个运行期间比例固定。若任一组试跑不通过，就将共同 `time_scale` 乘以 2、重新试跑两组；若正式运行不通过，也废弃这一整组对照，从空缓存重跑两组。不会将上一次 baseline 与重试后的 ReDNS 拼接。最多尝试 4 轮，仍不能承载则报错，保留之前的有效结果。单模型入口只接受固定数值，出现积压后拒绝保存为正常结果；独立 `auto` 已删除。

默认检查覆盖预热和正式窗口：所有请求的平均投放滞后不超过 0.01 模拟秒；按计划到达时间分成 1 模拟秒窗口，不能连续 3 个窗口平均滞后超过 0.05 秒；排空后台通知后，实际模拟时间不能超过计划窗口 0.1 秒以上。持续积压可提前中止无效试次。参数均在 `timing` 内：`max_mean_lag_seconds`、`max_window_lag_seconds`、`consecutive_lag_windows`、`max_overrun_seconds`。共同试跑默认预热最多 20 秒、正式最多 30 秒，由 `probe_warm_up_seconds` 和 `probe_duration_seconds` 设置；`calibration_requests` 可提高试跑正式长度，但不超过原正式窗口。重试由 `max_attempts`、`scale_multiplier` 控制。

两组复用同一份数值配置和 CPU 池，调度、各 client / forwarder 分片及 authority 对应绑定同一 CPU 槽位；ReDNS 的 manager 使用额外预留槽位，baseline 保持普通设计、不启动 manager。成对发布前校验完整的“计划时间、client、域名”序列 SHA-256、权威 TTL 表 SHA-256、请求数量、配置、缩放比例及公共分片的 CPU 和所有权信息完全一致，并检查运行期间源代码没有变化。结果中的 `workers` 按进程记录 PID、CPU、角色、分片编号及节点数。操作系统调度仍会带来小幅差异，积压检查限制其对模拟时间的影响。

增加核数提高可承载负载，不会自动缩短固定 `time_scale` 对应的计划运行时间。需要尝试加快实验时，可以使用 `python3 -m sim.run_comparison --time-scale 1`；两组仍采用共同速率，并在积压时共同放慢重跑。

## 结果与校验

成对入口默认输出到 `res/comparison/`：每次通过的对照保存在独立 `run-*` 目录，含两份 `*_results.json` 和 `manifest.json`；`comparison.json` 指向最新有效对照，记录共同配置、试跑/重试历史和两组摘要。两份结果全部通过后才更新指针，失败不覆盖旧结果。单模型入口仍默认输出 `res/baseline_results.json` 和 `res/ReDNS_results.json`。

结果包含逐请求延迟、每个节点及层级统计、权威查询量、目录通知量、转存生命周期、进程 PID / CPU、实际缩放比例和到达滞后。`timing_valid` 与 `timing_validation` 提供判定和逐秒滞后；`workload_fingerprint_sha256`、`ttl_fingerprint_sha256` 用于核验输入。结果标记 `schema_version=2`、`topology=client-forwarder-authority`。底层运行器保留无效试次的完整请求和诊断，供回归检查使用；正式入口不会发布它们。

`successful_reverse_queries` 表示成功反向查询次数：client A 的请求在 forwarder 本地未命中后，实际从另一个 client B 的缓存取回答案，且 A ≠ B。B 可以是直属 client，也可以属于同集群邻居。只在原请求 forwarder 计数，帮助方不重复计数。找到目录提示但查询失败、回查 A 自身、forwarder 缓存命中和权威回复均不计入。预热不计入正式结果，baseline 为 0。

该字段同时在终端输出、JSON 顶层、`forwarder.successful_reverse_queries` 汇总以及 `forwarders[i].successful_reverse_queries` 单节点统计中提供。总计等于各 forwarder 之和，不超过 `forwarder.peer_hits + forwarder.cluster_client_hits`。

集群统计包括 `cluster_queries`（定向目标探测次数）、`cluster_hits`、`cluster_misses`、`cluster_forwarder_hits` 及 `cluster_client_hits`。manager 查询另列 `manager_queries`、`manager_directory_hits/misses`，目录命中只表示找到候选，不等于实际取回记录。终端和 JSON 顶层均可查看这些计数；JSON 的 `manager` / `managers` 提供总计、每集群通知和版本反馈计数及目录规模，`manager_enabled` 标记是否启用独立 manager 进程。区域通知 `manager_updates_sent` 和失败反馈 `manager_feedback_sent` 经过队列并在结束时校验收发守恒。

JSON 的 `forwarder_clusters` 给出实际分组，每个 ReDNS forwarder 也记录 `cluster_id`。帮助方的 client 探测单列为 `cluster_child_queries/hits/misses`，不混入原请求的 `peer_queries/hits/misses`。manager 元数据和版本撤销记录不计为 DNS 缓存容量；目录按需清理到期项，版本撤销记录用于阻止迟到更新复活条目，其规模计入 `tracked_versions`。

完成前会排空前台请求、转存和目录通知，再自动校验查询守恒、peer 流量、通知收发、转存去向、接收阶段统计与容量上限，校验通过后才写结果。

对最新有效对照的 ReDNS 结果做离线回源拆解：

```bash
python3 -m sim.analyze_origin
```

该命令核对请求序列和 TTL 指纹，从保存的逐请求 RTT 还原回源请求，区分全局首次、集群首次及集群重复访问；同时重放无限容量、完美发现、即时响应的参考缓存。输出位于对应 `run-*` 目录下的 `origin_analysis/`。参考重放使用计划到达时间，不能直接当作历史过期、逐出或目录漏报的精确原因统计；原文件未记录完整缓存事件，报告会明确这些限制。命令不重新运行正式实验、不修改模型。

重构验证：68 项回归检查通过；另按 30,000 client、30 forwarder、15,000 请求/模拟秒运行了两组短验证，预热 5 秒、正式 10 秒、固定 time_scale=2，每组正式处理 148,734 次请求。配置、原始结果与汇总位于 `res/refactor_validation/`，不作为完整正式性能实验。

可按域名观察容量逐出、TTL 到期和有效驻留时间。以下命令保持当前拓扑、负载和缓存策略，预热 200 秒后观察 120 秒；覆盖全部 forwarder 和固定的 300 个 client 样本：

```bash
python3 -m sim.observe_retention --output-dir res/cache_retention_20261030 --warm-up-seconds 200 --duration-seconds 120 --time-scale 1
```

该观察默认关闭，普通实验入口不安装观察器。热门域名分别按各 forwarder 收到的 client 正常解析请求、所辖用户原始需求排名；后者离线重放同一请求生成器得到，不供模型决策使用。集群帮助请求的缓存访问会影响驻留，但不加入上述正常请求热度排序。原始数据为 `*_retention.json.gz`，报告为 `report.md`。观察器会增加执行开销，开启时的进程耗时不作为性能基准。

集群启用前的留存观察两组各完成 1,800,261 次正式请求，全部 client 的正式容量逐出均为 0。按 forwarder 实际入站查询排名，前 10 名的平均有效驻留比例约 99.5%，两组均只有 3 次容量逐出；更宽的热门集合仍会被逐出。完整历史数据见 `res/cache_retention_20261030/report.md`，其中配置省略集群大小，按大小 1 解释，不代表当前启用集群后的结果。

逐个探测邻居的历史验证保留在 `res/cluster_validation/`，它没有 manager，不代表当前版本。当前 manager 机制的实现检查覆盖合并区域条目、部分副本删除、过期、通知滞后、版本保护、候选定向和多进程队列守恒；尚未开展 600 秒完整性能对照。

manager 验证：120 项检查通过；保持 100 个 forwarder、集群大小 10 的短运行完成 148,734 次正式请求，manager 定位查询 44,353 次，只有 252 次实际定向邻居查询并全部取回记录，另外 44,101 次目录未命中没有扫描邻居。配置、原始结果和说明位于 `res/manager_validation/`。

共同时间尺度短验证：131 项检查通过；按当时 30,000 client、100 forwarder、集群大小 10、15,000 请求/模拟秒运行 20 秒预热及 40 秒正式窗口，共同比例 2。两组各完成 598,811 次正式请求，计划请求序列、权威 TTL 表、配置与公共 CPU 绑定核对一致，均无持续积压。配置、原始结果及时间统计见 [验证报告](res/fair_timing_validation/report.md)。后续完整 200 秒预热、600 秒正式对照保存在 `res/comparison/run-5ec515c8b5d2/`，含本轮分析和离线回源拆解。

多核分片验证：142 项检查通过；当前 30,000 client、1,000 forwarder、集群大小 100 的配置下，使用 4 / 8 / 2 个 client / forwarder / manager 工作进程，ReDNS 共占 16 个独立逻辑 CPU。两组以共同比例 1 完成 10 秒预热、20 秒正式窗口，各处理 299,205 次正式请求，无持续积压；详情见 [多核验证报告](res/parallel_validation/report.md)。

历史四层架构的结果保留在 `res/`，对应旧报告归档在 `res/history/previous_architecture/`。这些历史性能数字不代表本次三层模型的结果。当前机制的详细说明见 [ReDNS_ARCHITECTURE.md](ReDNS_ARCHITECTURE.md)。
