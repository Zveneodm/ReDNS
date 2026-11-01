# 目录与实验框架整理

本轮只调整模块归属、导入、配置选模与结果目录。缓存、TTL、替换、目录探测、并发 miss 合并、Forestall 候选和预取预算算法保持原有逻辑。

## 模块归属

| 旧路径 | 当前路径 | 职责 |
| --- | --- | --- |
| `src/prefetch*.py` | `src/auxiliary/prefetch*.py` | 可选预取算法、预算与协议 |
| `sim/arrivals.py`、`sim/preferences.py` | `sim/traffic/` | 合成流量与偏好 |
| `sim/config.py`、`sim/clock.py` | `lib/` | 配置、时钟 |
| `sim/realtime.py`、`sim/parallel.py` | `lib/` | 所有者进程、消息路由 |
| `sim/comparison.py`、`sim/experiment.py` | `lib/` | 对照编排、公共启动辅助 |
| `sim/metrics.py`、`sim/timing.py`、`sim/progress.py` | `lib/` | 校验、时序、进度 |
| `sim/retention.py` | `lib/retention.py` | 缓存留存观察器 |
| 原回源、留存、预取报告辅助函数 | `lib/analysis/` | 离线分析与报告 |
| 实验、观察和报告的命令行入口 | `sim/` | 保留原启动命令 |

`src/` 保留核心模型及协议，不导入实验进程或流量生成器。`src/auxiliary` 仍不依赖运行器；流量生成器只向实验层提供相同的种子请求流。源码一致性检查现覆盖 `src/`、`sim/` 与 `lib/`，避免迁移到 lib 后漏掉运行代码变动。

## 配置开关

```json
"prefetch": {
  "enabled": true,
  "budget_ratio": 0.15,
  "max_inflight_per_forwarder": 8
}
```

普通入口中，`true` 选择现有的 `baseline-prefetch` / `ReDNS-prefetch`，`false` 选择 `baseline` / `ReDNS`；省略开关默认关闭。当前根目录配置显式设为 `true`，其他标准实验参数未改。`run_prefetch.py` 和 `run_baseline_prefetch.py` 是相应普通入口的兼容别名，同样服从配置开关。

`--include-prefetch` 和 `--prefetch-ablation` 保留为显式对照集合选择，允许同一轮实验比较指定变体。底层协议及回归检查仍可按已有模型名称直接选择变体；结果中的 `prefetch_enabled` 记录实际模型能力。

## 结果目录

默认每次启动分配一个独立目录，例如 `res/20261101-1/`、`res/20261101-2/`、`res/20261101-3/`。日期取运行机器的本地日期，序号按数字递增，并使用独占创建避免并发重名。

默认对照的布局为：

```text
res/20261101-2/
├── config.json
├── baseline-prefetch_results.json
├── ReDNS-prefetch_results.json
├── manifest.json
├── comparison.json
└── report/                     # 另行运行报告入口后生成
```

各模型全部通过后才发布 `comparison.json`；离线分析默认找最新的已完成日期对照，跳过单模型目录和未完成目录，并保留旧 `res/comparison/comparison.json` 的回退读取。单模型保存实际变体名称的结果文件和配置快照。显式 `--output` / `--output-dir` 按用户给定的位置保存；显式对照目录保留原有 `run-*` 原子指针发布方式，不覆盖历史对照文件。

## 不变性与集成验证

202 项测试全部通过，覆盖原有算法、跨进程通信及新增的配置选模、日期编号并发分配、数字排序、旧目录读取、默认平铺发布和异地工作目录启动。

迁移前后，使用四种模型 × 两种替换策略，在手动时钟下运行相同的 8 组负载，包含并发同键请求、预热/正式阶段、目录通知、预算和预取执行。查询答案、节点统计、manager 统计和权威查询数导出后逐字节相同：

```text
before = 8de68ac5a57c92ed3c94ad37273171141c1d8a45711eca0db4fcd3e0c3b8458c
after  = 8de68ac5a57c92ed3c94ad37273171141c1d8a45711eca0db4fcd3e0c3b8458c
```

检查脚本：

```bash
python3 -m unittest discover -s tests -v
python3 experiments/structure-refactor/check_equivalence.py /tmp/redns-equivalence.json
```

实际短运行使用 `smoke_off.json` 和 `smoke_on.json`，24 client、4 forwarder、120 QPS、预热 1 秒、正式 3 秒，均通过标准时序门限。验证生成了：

- `res/20261101-1/`：关闭预取，baseline / ReDNS 对照，每组 391 次正式请求。
- `res/20261101-2/`：开启预取，baseline-prefetch / ReDNS-prefetch 对照，每组 391 次正式请求，并生成报告。
- `res/20261101-3/`：开启预取的单模型 ReDNS，391 次正式请求。

这三次运行用于验证结构与开关，不更新先前 200+600 秒标准实验的性能结论。历史 `res/prefetch-ablation/run-44d4374192c2/` 未移动；使用迁移后的验证工具重新读取，两组各 8,998,486 次请求的配置、流量、回源标记和前台 RTT 账本再次通过。旧配置缺少新开关时，验证工具按默认值归一化比较，不改写历史数据。

机器可读验证记录见 `verification.json`。
