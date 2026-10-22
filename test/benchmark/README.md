# FeedFlow 性能基准测试套件 (Performance Benchmark Suite)

本目录归档 FeedFlow 核心算力单元的官方性能基准快照、对比工具与操作入口。

## 目录结构

- [`baseline.txt`](./baseline.txt): 官方版本化基准测试快照数据（5 轮采样，兼容 `benchstat`）。
- [`baseline.md`](./baseline.md): 官方基准快照的人类可读指标看板与 SLA 达标分析。
- [`run.sh`](./run.sh): 基准测试套件启动器（支持 `--smoke`, `--record`, `--compare`）。
- [`compare.py`](./compare.py): 零依赖基准对比分析器，支持自动计算指标变化率并核验 SLA 预算与零内存逃逸门禁。

## 快捷运行

在仓库根目录直接执行：

```bash
# 1. 轻量 Smoke 校验（与 CI 保持一致）
./run_benchmark.sh --smoke

# 2. 现场采样当前代码并与官方基线比对
./run_benchmark.sh --compare test/benchmark/baseline.txt

# 3. 重新录制官方基线快照
./run_benchmark.sh --record test/benchmark/baseline.txt
```
