# ReDNS 实验

标准库实现的 DNS 缓存模拟器，使用 Python 3.9+，无需安装第三方依赖。

查询路径为 `Client → Forwarder → PublicDNSServer → AuthoritativeServer`。
每个 client 都有独立的缓存，固定连接一个 forwarder；多个 forwarder 共享一个 public DNS。
client 按编号轮流分配给各 forwarder，例如 5 个 forwarder 时，client 0 和 client 5 都连接 forwarder 0。

## 运行

在项目根目录运行：

```bash
python3 sim/run_baseline.py
python3 sim/run_ReDNS.py
python3 -m unittest discover -s tests -v
```

也支持直接执行脚本、指定配置和输出文件：

```bash
python3 -m sim.run_baseline --config config.json --output res/custom_baseline.json
python3 -m sim.run_ReDNS --config config.json --output res/custom_ReDNS.json
```

运行过程中只在同一行更新 `300s/3600s` 形式的进度，分母为预热与正式窗口之和。
请求调度积压时按已发出的计划到达进度显示，全部前台请求和后台消息收尾后才显示完成；结束时保留结果汇总。

默认配置路径相对于项目根目录确定，不受运行目录影响。
配置中的 `output_dir` 相对于项目根目录；命令行 `--output` 的相对路径相对于运行目录。
导入模块不会自动读取配置或启动实验。

实验编排（拓扑建立、查询生成、模拟时钟、统计）位于 `sim/run_baseline.py`，
`src/baseline/Worker.py` 只负责通过指定 client 发送查询。
`sim/run_ReDNS.py` 使用相同负载和统计流程，并接入 `src/Re_DNS/Model.py` 中的 ReDNS 模型。
默认入口使用 `sim/realtime.py`：四个独立进程分别拥有全部 client、全部 forwarder、public DNS 和权威数据。
缓存对象不跨进程共享，通过有界消息队列传递查询、回复和后台转存；等待上游时仍可处理其他消息。
Linux/WSL 上默认将四层绑定到不同的可用逻辑 CPU；可用 CPU 少于四个时复用，其他平台由操作系统调度。
父进程只协调启动、自动校准、统计和退出，不承担某层的查询处理。
ReDNS 将 public DNS 的 LRU 逐出记录转存到已连接的 forwarder，将 forwarder 的逐出记录转存到已连接的 client。
启用 `regional_preferences` 后，上游只观察下游实际转发的缓存 miss，使用有界、时间衰减的频率表学习偏好，
并按 `1-exp(-预计请求率*剩余TTL)` 选择最可能在到期前复用且没有已知有效副本的下游。
缓存替换、LRU 驱逐或过期会通知上游更新副本提示；接收端仍会原子检查重复记录。
没有足够学习证据时才通过域名 CRC32 稳定哈希选择下游，不占用实验的随机数序列。
记录以剩余 TTL 插入下级缓存的 MRU 位置，正常参与命中和倒计时；下级容量不足时继续向下转存，client 为末级。
剩余 TTL 小于 `min_transfer_ttl` 时向上游查询一次；响应 TTL 仍不足则以阈值存放，`auth_TTL` 保持权威值。
刷新查询可命中上游缓存，但沿途未命中时不回填缓存，以免触发循环驱逐。

## 真实时间与请求到达

`execution_mode` 默认为 `realtime`。四层初始化后使用共同的 `perf_counter_ns()` 起点，
模拟时间为 `(当前真实时间 - 起点) / time_scale`。TTL 连续减少，不再按整秒推进。
跨层传递绝对到期时间，排队和进程间通信所经过的时间不会延长记录的有效期。

`queries_per_second` 是所有客户端合计的泊松到达强度 λ：相邻请求间隔独立服从指数分布 `Exp(λ)`。
每次请求均匀选择一个 client。未启用区域偏好时，域名服从全局 Zipf 分布；启用后，每个区域有独立的域名排序，
同一区域的 client 从区域 Zipf 先验中形成相互不同但共享热点的完整排序，并按更集中的 client Zipf 指数采样。
因此每秒请求数会波动，
正式请求总数也不再固定等于 `duration_seconds * queries_per_second`。
到达间隔、client/域名选择和权威 TTL 使用独立的种子随机流。

`time_scale` 的单位是“每模拟秒占用的真实秒数”。例如 `0.0001` 表示模拟 1 秒使用真实 0.1 ms：

```bash
python3 sim/run_baseline.py --time-scale 0.0001
python3 sim/run_ReDNS.py --time-scale 0.0001
```

默认 `time_scale: "auto"` 会先用不填充缓存的请求测量四层吞吐，清除校准计数，再以测得吞吐的 70% 选择缩放比例。
比例同时受 `min_time_scale` 和单调时钟精度约束，实际采用的值写入 `actual_time_scale`。
这不是硬实时保证：5,000 请求/模拟秒在 `0.0001` 比例下需要处理 5,000 万请求/真实秒，
进程间通信吞吐也必须足够。对比两种算法时，可以先校准，再为两组实验指定相同、可承载的比例。

最多同时保留 `max_in_flight` 个未完成的客户端请求。负载超出处理能力时保留全部计划请求并按顺序补发，
不修改泊松样本或丢弃请求；实际发出时刻可能滞后，通过 `arrival_lag_mean_wall_ms`、
`arrival_lag_max_wall_ms` 和 `arrival_lag_mean_sim_seconds` 检查。等待全部前台请求及后台消息完成后才输出结果。
`observed_simulated_seconds` 包含处理积压时实际流逝的模拟时间，可能超过预热加正式到达窗口。

相同种子提供相同的计划到达和域名/client 序列；真实进程调度会影响缓存的访问时刻，结果不承诺逐位重现。
原来的整秒串行实现保留为 `DeterministicSimulation`，可用于回归对照：

```bash
python3 sim/run_baseline.py --execution-mode deterministic
python3 sim/run_ReDNS.py --execution-mode deterministic
```

## 模拟约定

- 三层缓存均使用 LRU。访问过期记录时删除并继续向上游查询；容量不足时先清理过期记录，再淘汰最久未使用的记录。容量设为 0 可关闭对应层缓存。
- 每层缓存保存独立的 `Record` 副本。下游继承上游响应的**剩余 TTL**，命中不会刷新 TTL；TTL 为 0 的响应可返回，但不缓存。
- 缓存记录插入时保存到期时间，读取剩余 TTL 时计算 `max(0, expires_at - current_time)`，无需遍历缓存记录。真实时间模式使用缩放后的连续单调时间，串行对照模式使用整秒时钟；两者都不访问外部 DNS。
- baseline 和 ReDNS 均使用到期时间计算缓存 TTL；预热和正式阶段共用同一时钟。权威记录和返回的响应副本不绑定时钟，`auth_TTL` 不随时间变化。独立使用模型时仍支持 `countdown()`。
- 实验内每个缓存使用到期时间最小堆定位过期项，容量不足时只处理到期的堆条目，无需扫描整张缓存。版本号区分被替换或逐出的旧条目；直接修改缓存记录 TTL 也会更新索引。旧堆条目定期压缩，压缩不删除缓存记录，不改变惰性清理时机、LRU 顺序或统计口径。未绑定模拟时钟的独立模型保留原有倒计时和扫描逻辑。
- `TTL` 是剩余 TTL，决定缓存是否可用；`auth_TTL` 是权威配置的 TTL 时长，始终保持常数，不参与倒计时。比如权威 TTL 为 60 秒，缓存 10 秒后两者分别为 50 和 60；下游获得的也是 50 和 60。保留 `TTL`/`auth_TTL` 和 `ttl`/`auth_ttl` 两套字段访问方式，不存储 IP 等记录内容。
- 每个域名启动时按 `ttl_distribution.values` 和 `probabilities` 抽取一个固定权威 TTL（秒），之后不重新抽样。只有重新查询权威服务器才取得完整 TTL。权威数据本身不倒计时。
- TTL 抽样使用独立的随机数生成器；相同种子下 baseline 和 ReDNS 的域名 TTL 分配一致，也不会影响查询序列。各档记录比例是概率抽样的结果，不保证数量恰好等于所填比例。
- `warm_up_seconds` 指定预热窗口，预热缓存会保留。真实时间模式以计划到达时间区分预热和正式请求，相关回复、转存和刷新沿用该请求的阶段标记，避免跨窗口的迟到回复污染计数。串行对照模式仍在预热结束时清零计数。
- 域名为合成的 `domain-00000.example` 等。关闭区域偏好时 Zipf 热度按域名编号排列；开启时各区域使用由种子确定的不同排序。真实时间模式使用全局泊松请求流；串行对照模式每秒产生固定数量的请求。
- 当前将递归解析抽象为权威记录查询，不模拟根/TLD 转介、并发请求合并、负缓存或域名内容变更。
- 网络延迟按配置的 RTT 累计，单位为毫秒；RTT 已包含往返，不再乘 2。本地缓存命中的网络延迟为 0，不计 CPU 处理时间。
- 请求解析延迟仅累计该 DNS 请求实际查询上游所经过的配置 RTT，不人为等待该 RTT。ReDNS 转存和转存前的刷新属于后台预置维护，其全部链路 RTT 不计入请求解析延迟，相关流量仍保留统计。真实时间下测得的进程往返耗时单独输出，包含实际执行、排队、进程通信和调度开销；TTL 随真实时间持续减少。

## 配置

`config.json` 是合法 JSON。省略的字段使用默认值；未知字段及无效数值会报错。

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `seed` | 42 | 控制计划到达、client/域名选择和 TTL 抽样；真实调度结果可能变化 |
| `execution_mode` | "realtime" | 四层真实时间模式；"deterministic" 使用原串行对照模式 |
| `time_scale` | "auto" | 每模拟秒占用的真实秒数，或自动校准 |
| `min_time_scale` | 0.0001 | 自动校准的最小比例 |
| `max_in_flight` | 512 | 同时未完成的客户端请求上限，正整数 |
| `calibration_requests` | 1000 | 自动校准使用的完整查询数量，正整数；不计入实验统计 |
| `pin_cpu` | true | 支持 CPU affinity 时将层进程绑定到逻辑 CPU |
| `record_wall_latencies` | true | 保存逐次真实进程往返耗时；关闭后仍计算平均值和最大值 |
| `warm_up_seconds` | 0 | 预热秒数，非负整数；预热不计入正式统计 |
| `duration_seconds` | 300 | 模拟时长，正整数秒 |
| `queries_per_second` | 100 | 全部 client 合计的泊松平均到达率（请求/模拟秒），允许 0 |
| `num_clients` | 100 | client 数量，正整数 |
| `num_forwarders` | 5 | forwarder 数量，正整数 |
| `client_cache_size` | 100 | 每个 client 的缓存容量（记录数） |
| `forwarder_cache_size` | 1000 | 每个 forwarder 的缓存容量 |
| `pub_cache_size` | 10000 | public DNS 的缓存容量 |
| `min_transfer_ttl` | 30 | ReDNS 转存记录的 TTL 阈值（非负整数秒），0 表示不因 TTL 较短而刷新；baseline 不使用 |
| `client_forwarder_RTT` | 1 | client 与 forwarder 之间的往返延迟，毫秒 |
| `forwarder_pub_RTT` | 10 | forwarder 与 public DNS 之间的往返延迟，毫秒 |
| `pub_auth_RTT` | 50 | public DNS 与权威服务器之间的往返延迟，毫秒 |
| `num_domains` | 2000 | 合成域名数量，正整数 |
| `ttl_distribution` | null | 权威 TTL 的离散分布，含等长且非空的 `values` 和 `probabilities`；TTL 为非负整数秒，概率在 [0, 1] 内且总和为 1 |
| `ttl_range` | [30, 300] | 未配置 `ttl_distribution` 时使用的均匀整数 TTL 范围；配置离散分布后以离散分布为准 |
| `query_distribution` | "zipf" | `zipf` 或 `uniform` |
| `zipf_alpha` | 1.1 | Zipf 指数，有限正数，越大越集中于热门域名 |
| `regional_preferences.enabled` | false | 启用分区域、分客户端的 Zipf 排序及上游在线偏好学习；要求 `query_distribution` 为 `zipf` |
| `regional_preferences.region_zipf_alpha` | 1.2 | 生成客户端排序时使用的区域热点先验指数 |
| `regional_preferences.client_zipf_alpha` | 1.5 | 每个客户端实际采样的 Zipf 指数，通常应大于区域指数 |
| `regional_preferences.shared_rank_count` | 64 | 每个客户端从区域先验无放回选取的头部排名数量；超过域名数时自动截断 |
| `regional_preferences.learning_half_life_seconds` | 600 | 上游观测频率的指数衰减半衰期（模拟秒） |
| `regional_preferences.max_learned_domains_per_child` | 64 | 每个下游最多保留的偏好域名数，限制学习表内存；大规模客户端实验可进一步调低 |
| `output_dir` | "res" | 实验结果目录，相对于项目根目录 |

## 结果

终端显示查询数、各层命中率和权威查询量，JSON 还包含每个 client/forwarder 的独立统计。
两个实验都在终端输出 `average_latency_ms`，并在 JSON 中保存 `request_latencies_ms`（按请求产生顺序排列）、
`total_latency_ms` 和 `average_latency_ms`。平均延迟为所有工作负载请求的延迟总和除以请求数，包含 0 延迟的本地命中；无请求时为 0。
真实时间模式另输出 `request_wall_latencies_ms`、`average_wall_latency_ms` 和 `max_wall_latency_ms`，单位均为未经缩放的真实毫秒。
两组逐次延迟按客户端发出请求的顺序保存，而不是按回复完成顺序保存。关闭 `record_wall_latencies` 可减少大实验的内存和结果文件体积。
结果还包含四个层进程的 PID/CPU、实际缩放比例、真实运行时间、计划请求调度滞后和实际流逝的模拟时间。
大规模逐次延迟使用紧凑数组并分块写出 JSON，避免构造另一份完整列表和巨大的 JSON 字符串。
默认 RTT 下，client、forwarder、public DNS、权威服务器命中对应的请求解析延迟分别为 0、1、11、61 ms，转存和后台刷新不增加这些延迟。
默认分别保存为 `res/baseline_results.json` 和 `res/ReDNS_results.json`；结果中的
`experiment` 字段标明实验类型。只有模型成功完成实验后才写入结果。
`hit_rate` 的分母是**到达该层的查询数**；`expired_evictions` 为实际清除的过期记录数，
`lru_evictions` 为容量压力下淘汰的有效记录数。过期记录采用惰性清理，
所以 `cache_entries` 包含尚未清除的过期项，`live_cache_entries` 仅统计有效项。

统计应满足：client 未命中数 = forwarder 查询数；forwarder 未命中数 = public DNS 查询数；
public DNS 未命中数 = 权威查询数。

ReDNS 还记录 `records_transferred`（转存到下级的记录数）、`records_received`（接收的记录数）
和 `refresh_queries`（转存前发起的刷新查询数）。刷新查询计入上游流量，但不计入请求解析延迟，因此 ReDNS 应满足：
每层 `upstream_queries = misses + refresh_queries`；client 的 `upstream_queries` 等于 forwarder 的 `queries`，
forwarder 的 `upstream_queries` 等于 public DNS 的 `queries`，public DNS 的 `upstream_queries` 等于权威查询数。

偏好策略另输出 `public_dns_preference_learning` 和 `forwarder_preference_learning`。其中拟合的 Zipf 指数及 R²
只基于上游可见的 `forwarded_cache_misses`，不是生成器中的真实偏好，也不保证混合流量严格服从 Zipf。
`preference_selected_transfers`/`preference_fallback_transfers` 区分学习选择与哈希回退。
转存生命周期通过 `transfers_accepted`、`transfer_duplicates`、`transfer_expired_in_transit`、
`transfer_hits`、`transfer_records_used`、`transfer_unused_expired`、`transfer_unused_evicted`、
`transfer_unused_replaced`、`transfer_unused_live` 和 `transfer_unused_expired_pending` 统计。
`transfer_use_rate` 是至少被前台请求使用一次的已接收转存记录比例；`transfer_waste_rate` 是尚未使用就已过期、
被驱逐或替换的比例，仍在缓存中的记录不提前判定为浪费。
