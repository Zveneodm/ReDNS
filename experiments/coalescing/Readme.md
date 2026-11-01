# 并发 miss 合并验证

原实现的确定性复现：client 0、2 连接同一 forwarder，同时查询其尚未缓存的域名，baseline、ReDNS、ReDNS-prefetch 均产生 2 次权威查询。修复后，同样输入只产生 1 次权威查询，两个 client 各收到一次相同版本的记录。

实现位于 `src/protocol.py`。每个 forwarder 所有者维护 `(forwarder_id, cache_key)` 在途表，当前缓存键为域名 ID。首个请求执行完整解析，后续请求共享结果；forwarder 回填一次，各 client 分别回填且保留原绝对到期时间。不同 forwarder、不同键各自解析。帮助查询仍只查缓存与直属 client，不等待前台解析，避免循环等待；后台预取独立执行。

合并等待者仍计入前台 `queries/misses`、预取热度和预算需求，不计入缓存命中或实际回源。等待者仅累计加入时尚未完成的解析路径 RTT。`request_authority_queries` 按正式请求顺序保存实际回源标记，避免仅凭 111 ms 时延将共享回源的等待者误判为另一次权威查询。

## 自动化检查

```bash
python3 -m unittest discover -s tests -v
```

192 项检查通过。新增回归覆盖三个模型、100 个等待者、不同 forwarder/缓存键隔离、零 TTL、零缓存容量、直属 client/邻居 forwarder/邻居 client 来源、每个等待者的反向查询身份、迟到等待者剩余 RTT、预热与正式阶段交叉、解析表清理、跨 client worker 合并，以及真实分片进程中的请求和权威查询守恒。

100 个并发同键 miss 的确定性检查中，三种模型均为 1 次权威查询、99 次合并等待、100 次各一次回复。

## 短三组集成验证

```bash
python3 -m sim.run_comparison --include-prefetch --config experiments/coalescing/config.json --output-dir res/coalescing-validation
python3 experiments/prefetch/check_latency.py --directory res/coalescing-validation --output experiments/coalescing/latency_check.json
python3 -m sim.analyze_origin --comparison res/coalescing-validation/comparison.json --output-dir res/coalescing-validation/origin_analysis
```

验证配置为 300 个 client、6 个 forwarder、2 个集群、2/2/2 个 client/forwarder/manager worker、1,500 QPS、预热 3 秒、正式 8 秒、time_scale=1。缓存容量及 TTL 分布为缩小后的测试配置，详见 `config.json`；项目根目录正式配置未修改。三组每组均处理 11,824 次正式请求，输入指纹相同，均通过正式入口的时序与排空检查。

| 模型 | 前台权威查询 | 后台预取权威查询 | 合并等待请求 | 平均配置 RTT（ms） |
| --- | ---: | ---: | ---: | ---: |
| baseline | 4,794 | 0 | 0 | 45.073156 |
| ReDNS | 2,719 | 0 | 1 | 26.312077 |
| ReDNS-prefetch | 2,494 | 762 | 3 | 24.298545 |

这些数字用于验证实验框架兼容性，不是合并机制的性能收益估计，也不是原 200+600 秒正式实验的更新结果。模拟器只在配置 RTT 上累加时延，并不按该 RTT 休眠；是否重叠取决于实际消息解析何时完成。因此此次短运行的合并次数很少，不能代表真实网络中 110 ms 在途窗口的合并率。

各模型的逐请求权威标记之和均等于实际前台权威查询数；ReDNS 离线回源分析使用这些标记，指纹及回源总数核对通过。时延账本加入 `coalesced_wait_latency_ms` 后，与逐请求时延之和和保存的平均时延完全一致，后台预取 RTT 继续排除。ReDNS-prefetch 正式预取预算需求为 5,087，额度为 762，实际预取为 762，预算约束通过。

机器可读摘要见 `validation.json`，逐链路时延检查见 `latency_check.json`。原始结果保留于 Git 忽略的 `res/coalescing-validation/run-faa394e7e983/`。
