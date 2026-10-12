# FeedFlow 官方微观算法性能基准测试快照 (Official Benchmark Baseline)

本文件沉淀了 FeedFlow 核心算力单元（内存向量池余弦检索、本地轻量多特征精排、词法向量生成）在受控环境下的官方基线性能数据，作为系统持续迭代过程中的性能防退化标尺与验收门禁。

---

## 1. 测试环境与基准元数据 (Environment & Metadata)

- **采集时间**: 2026-10-12
- **操作系统**: Linux (amd64)
- **CPU 规格**: 12th Gen Intel(R) Core(TM) i7-12700H (20 vCPU)
- **Go 运行期**: `go1.26.4 linux/amd64`
- **采样策略**: 5 轮统计采样 (`-count=5`)，启用堆内存逃逸追踪 (`-benchmem`)
- **原始快照文件**: [`baseline.txt`](./baseline.txt)

---

## 2. 核心指标与 SLA 性能预算对比看板 (Benchmark Scorecard)

| 核心模块 (Module) | 基准测试项 (Benchmark) | 平均耗时 (Time/op) | SLA 性能预算 | 预算余量 / 达标状态 | 堆内存分配 (B/op) | 逃逸分配次数 (allocs/op) | 零内存逃逸门禁 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **视频向量池** (`internal/video`) | `BenchmarkVectorPool_SearchCosine` (单协程) | **1.89 ms** | ≤ 2.0 ms | **+5.5% 达标** ✅ | 17,902 B | 16 allocs/op | - |
| **视频向量池** (`internal/video`) | `BenchmarkVectorPool_SearchCosine_Parallel` (并行并发) | **263.84 μs** | ≤ 2.0 ms | **超预期 (7.5x 快)** ✅ | 17,869 B | 16 allocs/op | - |
| **本地多特征精排** (`internal/feed`) | `BenchmarkCalculateLocalRerankScore_50Candidates` | **7.60 μs** | ≤ 100.0 μs (0.1ms) | **超预期 (13.1x 快)** ✅ | **0 B** | **0 allocs/op** | **严格零逃逸 ✅** |
| **本地多特征精排** (`internal/feed`) | `BenchmarkLocalRerankVideos_50Candidates` (50候选完整重排) | **35.92 μs** | ≤ 100.0 μs | **超预期 (2.8x 快)** ✅ | 2,584 B | 5 allocs/op | - |
| **词法向量生成** (`internal/embed`) | `BenchmarkEncode_1536Dim/ShortTitle` | **5.39 μs** | ≤ 20.0 μs | **达标** ✅ | 6,533 B | 20 allocs/op | - |
| **词法向量生成** (`internal/embed`) | `BenchmarkEncode_1536Dim/TechnicalMedium` | **12.41 μs** | ≤ 30.0 μs | **达标** ✅ | 6,949 B | 53 allocs/op | - |
| **词法向量生成** (`internal/embed`) | `BenchmarkEncode_1536Dim/MixedLong` | **8.80 μs** | ≤ 50.0 μs | **达标** ✅ | 7,782 B | 84 allocs/op | - |
| **词法分词与哈希** (`internal/embed`) | `BenchmarkLexicalUnits` | **5.77 μs** | ≤ 10.0 μs | **达标** ✅ | 800 B | 52 allocs/op | - |
| **词法分词与哈希** (`internal/embed`) | `BenchmarkFNV1a` | **17.6 ns** | ≤ 50.0 ns | **超预期** ✅ | **0 B** | **0 allocs/op** | **严格零逃逸 ✅** |

---

## 3. 核心算法设计与门禁分析

### 3.1 内存向量池余弦检索 (`VectorPool.SearchCosine`)
- **规模与场景**: 在 2,000 条 1536 维候选向量规模下，单协程检索仅需 **1.89ms**，并行检索缩短至 **263.84μs**；
- **业务意义**: 彻底满足在线读链路对候选集余弦匹配必须控制在 2ms 以内的 SLA 预算要求，为后续多路召回与重排保留充足的耗时窗口。

### 3.2 本地多特征精排零内存逃逸 (`CalculateLocalRerankScore`)
- **严格零堆分配**: 在 50 条候选视频的多特征（相似度、点赞数对数衰减、发布时间半衰期衰减等）打分过程中，严格实现 `0 B/op` 与 `0 allocs/op`；
- **业务意义**: 高频刷流场景下，单机每秒承载数千 QPS 时不会产生任何临时打分堆对象，从根本上消除了高并发下的 GC Stop-The-World 毛刺与延迟抖动。

### 3.3 词法哈希向量生成 (`embed.Encode`)
- **吞吐表现**: 针对短标题、技术中长标题及混合长标题，生成 1536 维致密向量耗时稳定在 5μs ~ 13μs 区间；底层 FNV-1a 紧凑哈希仅需 **17.6ns** 且保持零逃逸。

---

## 4. 复现与基准更新规范

1. **本地快速 Smoke 校验**:
   ```bash
   ./run_benchmark.sh --smoke
   ```
2. **生成并覆盖当前基线快照**:
   ```bash
   ./run_benchmark.sh --record test/benchmark/baseline.txt
   ```
3. **将新改动与本基准快照进行比对**:
   ```bash
   ./run_benchmark.sh --compare test/benchmark/baseline.txt
   ```
