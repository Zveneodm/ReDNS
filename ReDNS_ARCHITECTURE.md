# ReDNS 当前模型架构

本文对应三层解析拓扑及 cluster manager 目录服务的多进程实现。下文说明 baseline 与 ReDNS 的共同基础；集群主动预取变体 ReDNS-prefetch 见 [PREFETCH.md](PREFETCH.md)。旧公共 DNS 层、偏好学习转存和 TTL 补足实现均已移除。

核心算法保留在 `src/`，预取扩展位于 `src/auxiliary/`，合成流量位于 `sim/traffic/`，实验基础设施与分析辅助代码位于 `lib/`；`sim/` 顶层只保留启动入口。普通入口根据 `prefetch.enabled` 选择已有算法变体。此轮整理不改变解析、TTL、替换、并发合并、预取候选或预算逻辑；默认新结果按 `res/YYYYMMDD-N/` 保存，历史结果路径保留。

## 1. 拓扑与进程

```text
多个 client → 各自固定的 forwarder → 同一个权威服务器
                    ↓
             本 forwarder 的 client 副本目录
```

client 编号为 0 到 N−1，连接到 `client_id % num_forwarders`。forwarder f 的直属 client 为 `range(f, num_clients, num_forwarders)`。同集群 forwarder 可通过消息请求对方的缓存或直属 client，目录仍由各 forwarder 独立维护。没有 client 的 forwarder 也可以存在。

`forwarder_cluster_size` 定义连续编号分组：大小为 3 时，0–2、3–5 等各成一组。组内通过 manager 定位可用区域，再定向查询；组外不查询。末组按实际剩余数量组成，大小为 1 关闭集群协作，超过总数则全部一组。当前 config.json 为 100 个 forwarder、集群大小 100，对应 1 个逻辑 manager。baseline 不启用它们。

client / forwarder 各按节点 ID 对工作进程数取模分片，每个节点的可变缓存只有一个所有者。manager 按 `(cluster_id × num_domains + domain_id) % manager_workers` 分片，同一条域名目录的更新、查询和反馈进入同一所有者；逻辑上的每集群一个目录由这些进程共同持有，只有静态成员信息重复，动态域名条目不复制。baseline 或所有集群均为单节点时不启动 manager。权威 TTL 表由一个权威进程持有。父进程协调运行；队列传输查询、通知及回复，各所有者不共享缓存对象，并按可用逻辑 CPU 绑定运行。

## 2. 代码职责

| 代码 | 职责 |
| --- | --- |
| `src/records.py` | 不可变 Record，保存 domain、expires_at、auth_ttl |
| `src/cache.py` | 两组共用的缓存、实际变更回调、转存接收与使用统计 |
| `src/expiry.py` | 带版本的到期最小堆；压缩过时堆条目 |
| `src/replacement.py` | OrderedDict LRU 或 O(1) 等概率随机替换 |
| `src/cooperation.py` | 直属下游副本目录与容量提示 |
| `src/cluster.py` | 静态分组、邻居列表及跨集群边界检查 |
| `src/manager.py` | 合并区域目录、版本更新和失败反馈保护 |
| `src/models.py` | Client、BaselineForwarder、ReDNSForwarder、AuthoritativeServer |
| `src/protocol.py` | 与进程无关的查询、目录通知和空位转存协议 |
| `lib/realtime.py` | 缓存和 manager 所有者进程、队列、调度与协调 |
| `lib/parallel.py` | 分片所有权、带所有者标识的 RPC 路由、节点及目录统计合并 |
| `lib/timing.py` / `lib/comparison.py` | 积压判定、共同试跑、成对重跑与输入一致性检查 |
| `sim/traffic/arrivals.py` / `sim/traffic/preferences.py` | 请求到达、域名/client 抽样和合成区域需求 |
| `lib/config.py` / `lib/experiment.py` | 配置校验、命令行和流式结果输出 |
| `lib/metrics.py` | 聚合统计与完成时一致性检查 |
| `lib/retention.py` / `sim/observe_retention.py` | 可选缓存留存观察与域名热度分组；不参与模型决策 |

`src/` 不导入 `sim/`，不负责进程启动或请求生成；模拟层通过协议的 `_send()` 实现消息传输。没有单进程正式实验入口，手动时钟只用于可控回归检查。

## 3. 查询流程

两组共同执行：

1. client 检查本地缓存；命中就返回。
2. client 未命中，查询固定 forwarder。
3. forwarder 检查本地缓存；命中就返回。
4. baseline 的 forwarder 未命中，直接查询权威服务器。

ReDNS 将第 4 步改为：

1. 检查本 forwarder 的副本目录，先清掉该域名的已到期提示。
2. 有有效提示时，选择尚未尝试过、client ID 最小的候选发起 peer 查询。
3. 接收端只检查自己的本地缓存，不调用完整解析流程，不继续查 forwarder。
4. peer 命中后返回其绝对到期时间与原始 TTL，forwarder 正常回填并回复请求方。
5. peer 未命中时删除旧提示，继续尝试剩余有效提示；每个 client 每次查找最多探测一次。
6. 本地没有可用副本后，向本集群 manager 查询一次。manager 只返回同组其他 forwarder 中有有效区域提示的候选，不提供副本位于 forwarder 还是 client 的分类。
7. 邻居先检查自己的缓存；未命中则仅根据自己的目录探测直属 client。帮助请求不再查询邻居或权威。
8. 邻居 client 命中后，帮助方正常回填自身缓存，再返回原请求 forwarder；双方保留原绝对到期时间。
9. 候选按 forwarder ID 排序、每个最多尝试一次。失败会反馈对应的目录版本；剩余候选继续尝试，全部失败后仅原请求方查询权威。
10. manager 没有候选时直接查询权威，不扫描无提示的邻居。集群只有本节点时跳过 manager，直接走权威路径。

四种模型的正常前台查询在 forwarder 本地 miss 后，先检查 `(forwarder_id, domain_id)` 在途表。首个请求登记并执行上述完整解析；后续请求仅加入等待者列表，不重复执行副本探测、manager 定位或回源。回复时删除在途项，forwarder 回填一次，再逐个回复请求 client。每个请求保留自己的正式/预热归属、反向查询身份和剩余 RTT；当前缓存键只有域名 ID。不同 forwarder 不共享在途表。邻居帮助查询不加入在途等待，以保持缓存查询终止性；预取也独立执行。

所有正常响应逐层回填。普通回填可以按替换策略淘汰有效记录；不挤占有效记录的限制仅适用于后台转存。

邻居请求通过 forwarder 角色的消息队列收发，即使全部 forwarder 由同一模拟进程持有，也不在发起方直接读取其他节点缓存。接收端校验发起方和帮助方必须是同组不同节点。帮助请求只检查自身与直属 client，限制了查找深度并避免循环。不同原始请求可以并发等待回复。

## 4. 副本目录维护

每个 ReDNS forwarder 维护独立的 `CopyDirectory`：

```python
by_name = {
    domain_id: {
        client_id: absolute_expiry
    }
}
child_counts = {client_id: known_entry_count}
entries = total_client_domain_pairs
```

目录只存元数据，不保存 DNS 答案，也不包含其他 forwarder 的 client。

- client 正常回填或成功接受转存之后发送 `cache_stored`。
- client 替换、淘汰或清理到期记录时发送 `cache_removed`。
- 转存因已存在有效同名记录而被拒绝时，重新报告已有记录的真实到期时间。
- 发送通知不等于登记成功，forwarder 收到消息后才更新目录。
- 到期采用惰性清理：查目录时按域名清掉过期提示，client 清理缓存时也会通知。
- 删除携带旧记录的到期时间；目录里已有更晚期限时不删除，避免迟到的旧删除或失败探测覆盖更新。
- 同一发送进程到同一目标的通知保持顺序，不是完整的分布式版本/故障恢复协议。

`child_counts` 是占用提示，并非每一时刻的精确空余容量。尚未清理的过期提示可能导致保守跳过；通知滞后也可能让发送方误判有空位，所以接收端必须再次检查。

baseline 不挂载缓存通知回调，也不创建目录。

每个集群的 manager 另维护一份合并区域目录：

```python
by_domain[domain_id][forwarder_id] = (region_deadline, version)
region_deadline = max(本 forwarder 的有效记录期限, 所有已知直属 client 副本的有效期限)
```

每个“域名—forwarder 区域”只占一个条目。manager 不保存答案、不存 client ID，也不区分 forwarder 缓存与 client 副本。请求方自身区域会被排除；目录按 forwarder ID 返回候选，不按副本所在层级排序。

forwarder 实际存入/删除记录、收到 client 缓存通知、或清除失效 client 提示后，重新计算该域名的区域期限。合并期限改变才发送 `region_update`；仍有任一有效副本时保留条目，最后一份已知副本消失时发布期限 0 撤销。正常缓存查询不会延长任何副本 TTL，区域期限只是现有副本期限的最大值。

manager 收到通知才更新目录，不能直接读取 forwarder 或 client 缓存。通知包含单调版本，旧版本不能覆盖新状态；撤销版本保留，阻止迟到的旧发布复活条目。查询时按 TTL 排除过期项。目标区域查询失败后也发布当前状态，并由请求方反馈所用提示版本；反馈只使相同版本失效，不能删除较新的发布。

manager 与本地目录都可能因通知滞后短暂漏报或产生失效提示。manager 无提示时允许回退权威；有提示时必须访问目标验证。元数据和撤销版本额外占用内存，规模分别记录为 `directory_entries`、`tracked_versions`，不混入 DNS 缓存容量。到期条目惰性清理，物理目录条目数可能包含尚未清除的过期项。

## 5. 空位转存

仅当 ReDNS forwarder 因容量不足淘汰一条有效记录时尝试：

1. 没有直属 client、client 容量为 0，或记录已过期，则结束。
2. 目录中已有该域名的有效下游副本，则跳过。
3. 按合成域名的 CRC32 确定起始位置，在直属 client 中循环寻找占用提示小于容量的节点。
4. 找不到空位提示就跳过；找到后发送转存消息，携带原始绝对到期时间。
5. client 收到后检查同名有效副本、到期时间与实际容量。
6. 容量满时先清掉到期记录；仍满则拒绝，不淘汰任何有效记录。
7. 接收成功才缓存并向 forwarder 登记。

转存不查询权威，不补足或延长 TTL，也不发送给邻居 forwarder 或其 client。client 是末级，正常替换也不会继续转存。转存记录插入 MRU 位置，随后按普通缓存规则被查询、过期、替换或淘汰。

## 6. TTL 与缓存

Record 保存整数域名 ID、绝对到期时间、权威原始 TTL，不保存 IP、RR 类型或可变内容。其字段不可修改。

```text
剩余 TTL = max(0, expires_at - 当前模拟时间)
```

权威 TTL 在初始化时按独立随机流为每个域名抽样，之后固定。新权威查询返回 `expires_at = now + auth_ttl`；缓存命中、peer 回复和转存保持原期限。TTL 为 0 的响应可以返回，但不缓存。

两组共享完全相同的缓存实现：

- 容量满时先通过最小堆清掉已到期记录，再采用 LRU / random 淘汰有效记录。
- 命中更新 LRU 顺序；随机替换的命中不影响随机受害者选择。
- 同名替换不额外淘汰其他域名。
- 每个缓存的随机替换种子独立，不消费到达、域名选择或权威 TTL 的随机流。
- 过期采用惰性清理；物理条目数可能包含未清理过期项。
- 堆版本区分同名记录的新旧实例；周期压缩只整理堆，不改变缓存淘汰时机。

## 7. 多核模拟与时钟

当前配置 `workers={"client":4,"forwarder":8,"manager":2}`，ReDNS 有 4 个 client、8 个 forwarder、2 个 manager、1 个权威及 1 个独立请求调度进程，共 16 个模拟进程；baseline 使用相同的 14 个公共进程，预留 manager 槽位空闲。CPU 不足时复用；client / forwarder 工作进程数按实际节点数封顶。manager 按域名分片，因此单一大集群也能使用多个 manager 核。

独立调度进程只产生一条全局请求序列，控制全局 `max_in_flight`，按原请求顺序保存延迟。client 只有一个工作进程时兼任调度，省去独立调度进程。省略 `workers` 的各项默认值均为 1，兼容三进程 / 四进程路径。调度 RPC 不增加配置 DNS RTT，真实执行和 IPC 耗时仍计入实际请求耗时。每个 RPC token 包含发起进程标识和本地序号，避免多个分片的序号冲突；回复返回原发起者。

各进程使用同一个 `perf_counter_ns()` 起点与缩放比例：

```text
模拟时间 = max(0, 当前单调时间 - 公共起点) / time_scale
```

`time_scale` 是每模拟秒对应的真实秒数。TTL 在等待消息、排队和处理过程中持续减少。处理积压会使实际流逝的模拟时间超过计划窗口，因此结果必须同时检查到达滞后和 `observed_simulated_seconds`。

正式比较使用 `python3 -m sim.run_comparison`。先让两组在共同的数值比例下执行完整协议试跑，包含正常缓存、逐出、目录通知和 manager 维护，通过后再分别启动新进程，从空缓存进行同长度的预热和正式运行。当前默认比例为 2；`--time-scale auto` 从 `timing.auto_initial_time_scale` 开始，两组始终共用同一比例。原有无回填吞吐校准及单模型独立 `auto` 已移除。

投放滞后按实际发起时间减去计划时间计算，覆盖预热和正式请求。默认总平均不超过 0.01 模拟秒；按计划时间划分的 1 秒窗口不能连续 3 个平均超过 0.05 秒；排空后实际时间超出预热加正式窗口不能超过 0.1 秒。任一条件不满足就判为无效，持续滞后可以提前终止。阈值位于 `timing`，结果记录 `timing_valid` 和含逐窗口统计的 `timing_validation`。

任一试跑或正式运行无效时，废弃整轮对照，将共同的比例乘以 `timing.scale_multiplier`（默认 2）并从头尝试两组，最多 `timing.max_attempts`（默认 4）次。比例仅在轮次之间改变，单次运行内不调整时钟、不丢弃计划请求，也不通过重置 TTL 消除积压。试跑预热默认最多 20 秒、正式默认最多 30 秒；`calibration_requests` 表示试跑正式窗口的最小期望样本预算，受原窗口长度限制。正式窗口仍做完整检查，短试跑不被视为长期稳定的保证。

两组共用同一份配置和固定 CPU 池，调度、公共角色各分片使用相同 CPU 槽位，manager 使用额外槽位；baseline 不额外运行目录逻辑。发布前检查请求序列 SHA-256（含预热的计划到达时间、client ID、domain ID）、权威 TTL 表 SHA-256、两阶段请求数、实际比例及公共分片的 CPU、角色、编号和节点数一致，并核验运行期间 `src/`、`sim/` 源码指纹不变。每轮正式进程和缓存独立，试跑不预填正式缓存；操作系统调度仍有微小差异。

通过的两份结果及说明整体保存到独立 `run-*` 目录，再原子更新 `comparison.json`；失败不发布不完整对照，不覆盖之前通过的数据。单模型命令接受固定比例，并拒绝保存未通过时间检查的结果。模型中的查找、回填、转存、TTL 和 RTT 规则不因上述实验调度检查而改变。

有界跨进程队列施加背压。进程等待上游回复时仍处理其他消息；同一目标阻塞时允许向其他目标继续发消息，但不改变同一目标的消息顺序。到达请求不会因背压丢弃，最多同时保留 `max_in_flight` 个前台请求，其他计划请求按顺序补发。

结束时先等待前台完成，再暂停所有进程检查：pending/outbox 均空，且所有进程发送数之和等于接收数之和。尚未排空就恢复处理，再次检查。最后统一冻结时钟并收集快照。

## 8. 负载与预热

请求使用全局泊松到达流，间隔服从 Exp(queries_per_second)，每次均匀选择一个 client。正式请求数是抽样结果，不强制等于到达率乘窗口长度。

到达间隔使用 seed+2，请求 client/域名选择使用 seed，权威 TTL 抽样使用 seed+1。两组使用同样的随机流规则。

未启用区域需求时，域名按全局 uniform 或 Zipf 抽样。启用 `regional_preferences` 后，各 forwarder 区域具有不同热点；各 client 有完整、不重复的域名排序，头部从区域先验无放回抽样，然后按 client Zipf 指数抽样。此模块仅生成需求，协议不会读取这些真实排序，也不存在在线偏好学习。

预热和正式阶段共享缓存与连续时钟。阶段按请求的计划到达时间确定，响应、peer 和转存沿用该标记。缓存通知也携带触发事件的阶段，保证跨窗口消息的维护计数守恒。

## 9. 延迟口径

配置包含四类链路 RTT，单位 ms，RTT 已包含往返：

- `client_forwarder_RTT`，当前 1。
- `forwarder_forwarder_RTT`，当前 2。
- `forwarder_manager_RTT`，当前 1。
- `forwarder_authoritative_RTT`，当前 110。

| 请求路径 | 累计网络 RTT |
| --- | --- |
| client 命中 | 0 |
| client → forwarder 命中 | 1 |
| client → forwarder → 另一个 client 命中 | 2 |
| manager 定位后，首个目标 forwarder 缓存命中，无前序失败 | 4 |
| manager 定位后，首个目标的 client 命中，无前序失败 | 5 |
| 无邻居、无 client 探测，直接查权威 | 111 |
| manager 没有其他区域提示，直接回退权威 | 112 |
| manager 返回两个过期前失效的区域，本地及两个目标各失败探测一个 client，再查权威 | 119 |

以上数字对应当前 RTT。一次 manager 定位计一段 forwarder-manager RTT，包括目录未命中。每次实际 client 探测加一段 client-forwarder RTT，每次实际目标区域探测加一段 forwarder-forwarder RTT，目标内部的 client 探测另计。失败也累加，没有候选的邻居不发查询、不计探测费用。

`average_latency_ms` 为上述前台配置 RTT 的平均值，没有人为 sleep 等待 RTT。`average_wall_latency_ms` 为真实处理、排队、IPC 和调度耗时，另列统计，不加上合成 RTT。

目录通知、区域发布、失效反馈和转存经过真实消息队列，因此会消耗 CPU / 队列并影响真实耗时。manager 查询 RTT 计入前台累计 RTT；维护通知未单独模拟网络带宽或传输等待，不将它们的后台 RTT 加到前台。该模拟不能直接预测真实部署的完整端到端响应时间。

## 10. 统计和一致性

`queries` 指正常到达该层的查询，不包含接收的 client peer 或集群帮助查询。`hits` 只算正常请求的本地缓存命中；`peer_hits` 指原请求直接查询直属 client 的命中。帮助方内部的 client 探测单列为 `cluster_child_queries/hits/misses`：

```text
queries = hits + misses
upstream_queries = misses - peer_hits - cluster_hits - coalesced_queries
peer_queries = peer_hits + peer_misses
cluster_queries = cluster_hits + cluster_misses
cluster_hits = cluster_forwarder_hits + cluster_client_hits
cluster_child_queries = cluster_child_hits + cluster_child_misses
client.upstream_queries = forwarder.queries
forwarder.upstream_queries = authoritative_queries
forwarder.peer_queries + forwarder.cluster_child_queries = client.peer_queries_received
forwarder.peer_hits + forwarder.cluster_child_hits = client.peer_hits_served
forwarder.cluster_queries = forwarder.cluster_queries_received
forwarder.cluster_hits = forwarder.cluster_hits_served
forwarder.cluster_misses = forwarder.cluster_misses_served
forwarder.cluster_forwarder_hits = forwarder.cluster_cache_hits_served
forwarder.cluster_client_hits = forwarder.cluster_child_hits
forwarder.records_transferred = client.records_received
client.directory_stores_sent = forwarder.directory_stores_received
client.directory_removes_sent = forwarder.directory_removes_received
forwarder.manager_queries = manager.queries
forwarder.manager_directory_hits = manager.directory_hits
forwarder.manager_directory_misses = manager.directory_misses
forwarder.manager_updates_sent = manager.updates_received
forwarder.manager_feedback_sent = manager.feedback_received
manager.queries = manager.directory_hits + manager.directory_misses
manager.updates_received = manager.updates_applied + manager.updates_ignored
manager.feedback_received = manager.feedback_applied + manager.feedback_ignored
```

`successful_reverse_queries` 单独统计成功反向查询：client A 请求在 forwarder 本地未命中，实际从另一个 client B 的缓存取回记录，A ≠ B。B 可以属于本 forwarder 或同集群邻居。请求携带来源 client ID，邻居回复携带实际来源 client ID；仅在原请求 forwarder 核对并计数，每次请求最多加 1，帮助方不重复计数。邻居自身缓存命中也不算 client 反向命中。

该计数沿用原请求的预热/正式阶段标记。终端与 JSON 顶层输出正式总计，`forwarder` 中为汇总，`forwarders` 中保留单节点值：

```text
successful_reverse_queries = sum(forwarders[i].successful_reverse_queries)
0 <= successful_reverse_queries <= (forwarder.peer_hits + forwarder.cluster_client_hits
                                   + forwarder.coalesced_peer_waits
                                   + forwarder.coalesced_cluster_client_waits)
```

`cluster_queries/hits/misses` 按实际邻居探测计数，一次原始请求可能探测多个邻居，但最多成功一次。JSON 顶层和 forwarder 汇总提供这些计数以及两类来源的命中数。帮助方的本地缓存命中计入 `cluster_cache_hits_served`，不混入正常请求的 `hits`。每个 forwarder 的 `cluster_id` 和顶层 `forwarder_clusters` 给出实际分组；baseline 分组列表为空、协作计数为 0。

`manager_queries`、`manager_directory_hits/misses` 统计定位查询；目录命中仅表示返回非空候选列表，不直接算作 `cluster_hits`。JSON 顶层与 forwarder 汇总保留它们；`manager` 和 `managers` 分别提供 manager 总计、每集群统计，`manager_enabled` 标记是否启动独立所有者。预热和正式消息沿用原阶段，物理目录状态则包含所有阶段留下的有效信息。

转存接收数划分为接受、重复、途中到期、容量满拒绝。已接受的转存按接收阶段核算：

```text
transfers_accepted =
    transfer_records_used
  + transfer_unused_expired
  + transfer_unused_evicted
  + transfer_unused_replaced
  + transfer_unused_live
  + transfer_unused_expired_pending
```

`transfer_records_used` 每条接受记录最多加一次；`transfer_hits` 可以多次增加。预热接收的记录在正式阶段被使用，可以增加正式 transfer_hits，但不增加正式接收组的 transfer_records_used。

`peer_transfer_hits` 只标记直接从转存来源条目读到的 peer 命中，不追踪正常回填之后的完整因果链。目录条目数和缓存占用是结束时物理状态，不按接收阶段分组。

运行完成后自动检查以上守恒式、每个节点容量和 IPC 排空状态；失败不写结果。JSON 使用 `schema_version=2` 和三层 topology 标记，避免与历史结果混淆。

## 11. 运行与当前边界

```bash
python3 -m sim.run_baseline --config config.json
python3 -m sim.run_ReDNS --config config.json
python3 -m unittest discover -s tests -v
```

只保留 baseline 与 ReDNS 两个实验入口；旧模型配置会被严格校验拒绝。完整参数和目录说明见 [Readme.md](Readme.md)。

manager 机制尚未开展 600 秒完整正式比较。历史四层、无集群及逐个扫描邻居的结果保留在 res 中，不作为当前 manager 模型的性能依据。

可通过 `python3 -m sim.observe_retention` 单独运行留存观察。观察器仅包装选中节点的查询、检查、存入和删除回调，保留原回调行为；普通实验不安装观察器。容量逐出、TTL 逻辑到期、同名替换及窗口结束仍有效分别计数；驻留时间在预热边界裁剪，并在 TTL 到期时停止累计，即使物理条目尚未清理。该功能增加 CPU 开销，不用于测量无观察器时的进程性能。

当前模拟不包含真实 DNS 报文、IP/RR 类型、根/TLD 转介、负缓存、并发查询合并、网络丢包、目录断线重建或权威内容更新。模拟进程按角色和分片分工，每个工作进程内部顺序处理消息，不代表每个虚拟节点拥有独立 CPU。
