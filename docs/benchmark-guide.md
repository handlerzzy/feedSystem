# FeedFlow 性能基准管理与比对操作指引 (Performance Benchmark Guide)

本文档规范了 FeedFlow 代码库的微观性能基准（Go Micro-Benchmarks）管理流程、持续集成流水线防退化策略，以及如何使用业界标准基准比对工具（如 `benchstat`）对改动前后的耗时与内存分配差值进行客观审查。

---

## 1. 核心目标与防退化原则

FeedFlow 在短视频推荐流中承担高并发毫秒级读链路服务。为了避免因日常功能迭代、依赖升级或逻辑重构导致的隐性性能衰退与内存逃逸，团队建立以下防退化原则：

1. **客观定量而非直觉猜测**：所有算力优化或核心改动必须依赖 Go 标准基准（`testing.B`）多轮采样的统计数据支撑，杜绝使用普通单元测试进行不稳定的单次墙上时钟度量；
2. **零堆分配硬性红线**：本地多特征精排打分核心路径（`CalculateLocalRerankScore`）与紧凑哈希算法（`FNV1a`）必须严格保持 **`0 B/op, 0 allocs/op`**。任何引入堆对象逃逸的 PR 一律阻断；
3. **SLA 性能预算门禁**：
   - 内存向量池 2,000 候选余弦检索：**≤ 2.0 ms**
   - 50 候选本地多特征打分：**≤ 100.0 μs (0.1 ms)**
4. **CI 烟雾守护与非阻塞原则**：持续集成（CI）公有虚拟机易受宿主 CPU 调度波动影响，因此 CI 仅执行轻量级 Smoke 可用性校验（`-benchtime=1x`）保证基准签名与代码无崩坏；性能比对作为开发者本地审查与 PR 准入的客观凭据。

---

## 2. 基线文件结构与版本化管理

官方微观算法基准快照保存在版本控制系统中：

| 文件路径 | 说明 | 格式 |
| :--- | :--- | :--- |
| `test/benchmark/baseline.txt` | 官方原始基准测试输出快照（多轮采样） | Go benchmark 标准文本输出（兼容 `benchstat`） |
| `test/benchmark/baseline.md` | 官方基准快照的结构化可读指标看板 | Markdown 报告，记录硬件规格、SLA 余量与分配分析 |
| `test/benchmark/compare.py` | 开箱即用的门禁比对解析器（含 SLA 与 0 逃逸判定） | Python 3 纯标准库脚本 |
| `test/benchmark/run.sh` | 基准测试与比对统一入口脚本 | Bash 脚本 |
| `run_benchmark.sh` | 仓库根目录快捷执行入口 | Bash 脚本 |

---

## 3. CI 流水线防退化校验

在持续集成流水线（`.github/workflows/ci.yml`）中，后端构建任务在常规单元测试后追加轻量级基准 Smoke 校验：

```yaml
- name: Test
  run: go test -race -count=1 ./...

- name: Benchmark Smoke Test
  run: go test -run='^$' -bench='^Benchmark' -benchmem -benchtime=1x ./...
```

### 校验目的
- **阻止破坏性重命名**：防止改动破坏现有基准函数的公开签名与参数；
- **阻止编译期与运行期回归**：确保全库所有 Benchmark 代码处于可随时编译、可正确运行且零 Panic 状态；
- **极速低负载**：全库 Smoke 仅耗时 ~2-3 秒，不增加 CI 队列负担。

---

## 4. 业界标准工具链：使用 benchstat 审查性能差异

Go 官方性能团队提供的 [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) 是行业公认的基准差异度量与统计显著性判定工具。

### 4.1 安装 benchstat
在开发机或测试环境中执行：
```bash
go install golang.org/x/perf/cmd/benchstat@latest
```
安装成功后，二进制位于 `$(go env GOPATH)/bin/benchstat`。建议将其所在目录加入系统的 `PATH` 环境变量中。

### 4.2 采集基准数据规范
基准测试必须采用多轮采样以消除系统偶发抖动。标准要求为 **5 轮或 10 轮**（`-count=5` 或 `-count=10`），并启用内存分配统计（`-benchmem`）：

```bash
cd backend
# 采集待测分支的性能数据并重定向保存
go test -run='^$' -bench='^Benchmark' -benchmem -count=5 \
  ./internal/video ./internal/feed ./internal/embed > candidate.txt
```

### 4.3 运行基准比对
使用 `benchstat` 对比官方基线快照与待测分支输出：

```bash
benchstat test/benchmark/baseline.txt candidate.txt
```

### 4.4 解读 benchstat 输出

典型的 `benchstat` 输出如下所示：

```text
goos: linux
goarch: amd64
pkg: github.com/handlerzzy/feedsystem/internal/feed
cpu: 12th Gen Intel(R) Core(TM) i7-12700H
                                                   │ test/benchmark/baseline.txt │       candidate.txt       │
                                                   │           sec/op            │   sec/op     vs base      │
CalculateLocalRerankScore_50Candidates-20                            7.602µ ± 5%   7.410µ ± 4%  ~ (p=0.421)
LocalRerankVideos_50Candidates-20                                   35.924µ ± 8%  34.801µ ± 6%  ~ (p=0.310)

                                                   │ test/benchmark/baseline.txt │       candidate.txt       │
                                                   │            B/op             │    B/op      vs base      │
CalculateLocalRerankScore_50Candidates-20                              0.0 ± 0%      0.0 ± 0%   ~ (p=1.000)
LocalRerankVideos_50Candidates-20                                    2.584k ± 0%   2.584k ± 0%  ~ (p=1.000)

                                                   │ test/benchmark/baseline.txt │       candidate.txt       │
                                                   │          allocs/op          │ allocs/op    vs base      │
CalculateLocalRerankScore_50Candidates-20                              0.0 ± 0%      0.0 ± 0%   ~ (p=1.000)
LocalRerankVideos_50Candidates-20                                      5.00 ± 0%     5.00 ± 0%  ~ (p=1.000)
```

#### 指标解读重点：
1. **`sec/op` (单次纳秒/微秒耗时)**：观察均值与百分比变动（如 `-2.52%` 为提升，`+12.0%` 为退化）；
2. **`B/op` 与 `allocs/op` (堆内存字节与分配次数)**：
   - 重点检查 `CalculateLocalRerankScore_50Candidates` 是否保持 `0.0`；
   - 任何由 `0.0` 变为 `>0` 的改动均视为严重的堆逃逸退化。
3. **`p` 值（统计学显著性检验）**：
   - `p > 0.05`（标记为 `~`）：说明新旧版本无统计学显著差异，属于正常的系统噪声；
   - `p < 0.05`：说明耗时或内存分配发生实质性显著变化。

---

## 5. 项目内置一键式比对审查命令

为了降低开发者的工具安装门槛，本项目在根目录提供了开箱即用的自动化封装脚本 `./run_benchmark.sh`：

### 5.1 现场采集并与官方基线自动比对
```bash
./run_benchmark.sh --compare test/benchmark/baseline.txt
```
> 脚本将自动在后台对当前工作区代码执行 5 轮采样，并自动调用环境中的 `benchstat`（若存在）及内置的 SLA / 零逃逸质量门禁分析器。

### 5.2 对比两份已保存的基准文件
```bash
./run_benchmark.sh --compare test/benchmark/baseline.txt candidate.txt
```

### 5.3 比对看板输出示例
```text
=========================================================================================
                         FeedFlow 微观算法性能基准比对审查看板                           
=========================================================================================
 基线快照 (Old): test/benchmark/baseline.txt
 当前改动 (New): candidate.txt
-----------------------------------------------------------------------------------------
基准测试项 (Benchmark)                          基线耗时       待测耗时       耗时变化      内存分配         堆逃逸变化        门禁裁定
-------------------------------------------------------------------------------------------------------------------
BenchmarkCalculateLocalRerankScore_50Candidates 7.60μs     7.60μs     +0.00%    0 B          0 ➔ 0        ✅ PASS
BenchmarkFNV1a                             17.6ns     17.6ns     +0.00%    0 B          0 ➔ 0        ✅ PASS
BenchmarkLocalRerankVideos_50Candidates    35.92μs    35.92μs    +0.00%    2584 B       5 ➔ 5        ✅ PASS
BenchmarkVectorPool_SearchCosine           1.89ms     1.89ms     +0.00%    17902 B      16 ➔ 16      ✅ PASS
BenchmarkVectorPool_SearchCosine_Parallel  263.84μs   263.84μs   +0.00%    17869 B      16 ➔ 16      ✅ PASS
-------------------------------------------------------------------------------------------------------------------
✅ 审查结论: 全部基准指标正常，SLA 与零逃逸门禁全部通过！
```

---

## 6. PR 审查与性能阻断红线 (Quality Gates)

当提交涉及核心读链路或算法优化的 Pull Request 时，评审人员按如下准则验收：

| 门禁类型 | 判定规则 | 审查动作 |
| :--- | :--- | :--- |
| **堆内存逃逸门禁 (Zero-Alloc)** | `CalculateLocalRerankScore` 或 `FNV1a` 出现 `allocs/op > 0` 或 `B/op > 0` | **一票否决 (Hard Block)**，必须定位并消除堆逃逸后方可合并 |
| **SLA 耗时预算门禁** | 余弦检索均值 > 2.0ms 或 精排打分均值 > 100μs | **一票否决 (Hard Block)**，算法复杂度未达工业级标准 |
| **严重耗时退化 (>15%)** | 关键路径耗时劣化显著（p < 0.05 且退化 > 15%） | **挂起审查 (Soft Block)**，作者需说明退化原因或权衡考量 |
| **微小波动 (±5%)** | 耗时变化在 ±5% 以内且 `allocs/op` 无新增 | **正常通过 (Pass)**，归类为日常运行期正常抖动 |

---

## 7. 官方基线快照的更新与发布规范

基线快照是全团队的共同标尺，严禁在未经讨论的前提下随意覆写。仅在以下场景下允许更新 `test/benchmark/baseline.txt`：

1. **重大算法正向优化落地**：如重构向量相似度计算实现翻倍加速，经评审确认后更新基线；
2. **底层运行时升级**：升级 Go Major 版本或基础工具链导致基准环境发生永久性跃迁；
3. **官方更新操作命令**：
   ```bash
   # 在干净的代码树上重新运行并落盘
   ./run_benchmark.sh --record test/benchmark/baseline.txt
   # 同步刷新 baseline.md 指标看板
   # 提交 PR，由核心维护者审查后合并
   ```
