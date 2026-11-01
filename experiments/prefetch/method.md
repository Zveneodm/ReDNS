# 实验方法

本次实验基于主分支提交 `06a5d72ca665291a8a8d98d174eab1047acc7535`，在新建的 `prefetch` 分支实施。实际代码校验值由对照入口写入结果 manifest，包含未提交的 Python 源码内容。

`config.json` 是仓库当前实验配置的副本。只显式增加预取参数，将进程配置设为 client=2、forwarder=4、manager=2，并从共同 `time_scale=1` 开始试跑。工作负载、种子、预热/正式长度、节点、缓存、TTL 和链路 RTT 均与项目配置一致。运行环境为 Ubuntu / WSL，10 个可用逻辑 CPU，约 11 GiB 内存；同一池的 CPU 分配用于三组实验。

```bash
python3 -m unittest discover -s tests -v
python3 -u -m sim.run_comparison --include-prefetch \
  --config experiments/prefetch/config.json --output-dir res/prefetch
python3 -m sim.report_prefetch res/prefetch --output-dir experiments/prefetch
```

正式对照前运行全部 152 项测试，包括 10 项新增的预取算法、消息协议、分片、多集群边界、低负载选择和真实多进程校验。`git diff --check` 与 Python 编译检查通过。

每轮先分别运行 20 秒预热加 30 秒正式窗口的完整协议试跑；随后三组分别从空缓存开始，运行 200 秒预热加 600 秒正式窗口。任何一组试跑或正式运行积压超标，整轮作废，时间比例翻倍后三组重跑。正式数据不与前一轮结果拼接。

同一随机种子的完整计划到达序列、client、域名与权威 TTL 表通过 SHA-256 校验；结果还检查节点所有权、公共 CPU、前后台消息守恒、缓存容量和排空。计时门限保持默认：全程平均投放滞后不超过 0.01 模拟秒，不允许连续 3 个一秒窗口平均滞后超过 0.05 秒，结束排空超时不超过 0.1 秒。

主要比较平均配置 RTT 延迟、延迟分位数、前台回源量和包含预取的总回源量。后台预取不混入用户查询命中率。`report.md` 和 `summary.json` 由完成并通过校验的三组结果生成；具体共同时间比例、指纹和结果目录以该文件为准。此实验使用一个 seed，不声称统计显著性或复现论文的性能增益。

完成正式对照后再次运行完整测试套件，最终共 153 项通过，其中 11 项为新增预取测试，包含补充的单节点集群检查。完整测试输出保存于 `tests.log`。三组都在第一轮通过，共同 `time_scale=1`，无需重试。结果及额外运行开销的解读见 [analysis.md](analysis.md)。
