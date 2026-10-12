# ADR-0002: 建立四层闭环性能测试体系（Go 微基准、容器化 k6 压测与 pprof 诊断）

## 状态
Accepted

## 背景与问题 (Context)
在 FeedFlow 语义召回与架构优化重构后，系统制定了严苛的性能指标红线（如内存向量池余弦打分 ≤2ms、本地多特征精排 ≤0.1ms、端到端读链路 P99 ≤25ms）。然而，当前代码库存在严重的性能测试断层：
1. **基准测试完全缺失**：代码中没有 Go 标准基准测试（`func Benchmark` 为 0），核心算力缺乏基于 `testing.B` 的纳秒耗时与堆内存分配（`allocs/op`）定量验收；
2. **单测耗时断言脆弱**：在单元测试中直接使用 `time.Since` 硬编码时间阈值，容易因 CI/CD 虚拟机的宿主 CPU 调度抖动产生 Flaky 误报；
3. **系统级宏观压测空白**：缺乏系统级并发压测工具，既有的 Postman 集合与 Shell 脚本仅覆盖单次功能回归，无法检验单机吞吐极限（QPS）与高并发下的 P90/P95/P99 延迟分布。

## 架构决策 (Decision)
建立贯穿“微观算法 ➔ 组合降级 ➔ 介观存储 ➔ 宏观全链路 ➔ pprof 诊断闭环”的四层性能测试金字塔体系：

1. **微观层（Micro Benchmark / Go testing.B）**：
   - 为内存向量池余弦检索（`VectorPool.SearchCosine`）、本地多特征精排打分（`LocalRerankVideos`）、词法哈希（`embed.LexicalVector`）建立标准基准测试；
   - 启用 `b.ReportAllocs()`，将核心精排路径的**零堆内存逃逸（`0 B/op, 0 allocs/op`）**作为质量门禁；
   - 将现有单元测试中的硬编码耗时测试迁移解耦至 Benchmark，单测仅保留宽裕的上下文超时降级防护。
2. **介观与存储层（Meso / verify.sh）**：
   - 固化针对 MySQL 关键业务表查询的执行计划与索引列一致性审计（杜绝 `type=ALL` 全表扫描和 `Using filesort` 临时表排序）。
3. **宏观系统层（Macro / 容器化 k6 压测）**：
   - 放弃自研 Go 压测轮子，直接采用官方 `grafana/k6` Docker 容器挂载执行压测脚本；
   - 压测容器直接加入 Docker 网络（`feedflow_default`），以服务名直接发压，实现宿主机环境零安装依赖；
   - 采用多梯度 SLA 预算分级度量真实业务链路：
     - **轻载基线 (≤50 并发用户)**：断言 `p(99) ≤ 25ms` 与 `p(95) ≤ 15ms`；
     - **常态高负荷 (200 并发用户)**：断言 `p(99) ≤ 100ms`，吞吐 `>1,200 QPS`；
     - **单机极限冲击 (500 并发用户)**：验证高并发下单机连接池健康度与高可用兜底，断言请求失败率 `< 0.1%`，吞吐 `>1,500 QPS`。受限于单宿主机容器网络协议栈与协程排队论延迟下限（Little's Law），单机极限 P99 上限放宽至 `≤ 400ms`。
4. **读链路 L1 进程内缓存与可测性开关 (L1 LocalCache Toggle)**：
   - 为避免 500 VU 并发瞬时击穿 MySQL 连接池，在 `FeedService` 中对高频只读元数据（视频标签、用户近期点赞 ID、AI 摘要、探索候选）引入纳秒级进程内短 TTL 缓存（2~10 秒）；
   - 提供环境开关 `FEED_LOCAL_CACHE_ENABLED` 与编程式方法 `WithLocalCache(bool)`，使压测套件可自由在“纯穿透无缓存”与“真实热缓存”两种拓扑间切换比对。
5. **诊断闭环（Diagnostic Loop / pprof）**：
   - 依托独立 Pprof Server（`:6060`），压测启动器支持 `--profile` 参数，在高并发压测活跃期自动抓取 CPU 火焰图、堆内存快照与 Goroutine 栈帧，联动微观 Benchmark 精准定位锁竞争、I/O 阻塞或序列化热点。
6. **CI 门禁策略**：
   - CI 环境仅执行 `-benchtime=1x` 运行不崩溃校验并生成基线对比报告（`benchstat`），不因公有虚拟机的时间波动强行阻断 PR。
7. **数据库连接池扩容与索引前置（DB Pool & Index Prerequisites）**：
   - **连接池容量治理**：在 `internal/db/db.go` 中将底层 SQL 连接池扩容至 `MaxIdleConns=100` 与 `MaxOpenConns=300`（并配置 `ConnMaxLifetime=1h`, `ConnMaxIdleTime=10m`），作为 2000+ QPS 极限发压的架构前置条件，彻底根治短连接频繁重连造成的 `TIME_WAIT` 端口耗尽（`cannot assign requested address`）；
   - **点赞历史复合索引**：在 `internal/video/like_entity.go` 为 `Like` 实体建立 `idx_like_account_created (account_id, created_at)` 复合索引，消除用户即时画像召回路径中查询近期点赞视频的 `Using filesort` 临时表排序瓶颈，使高并发读链路直接实现索引覆盖扫描。

## 权衡与影响 (Consequences)
- **优势**：
  - 彻底打通“宏观压测暴露瓶颈 ➔ pprof 火焰图定位根因 ➔ 微观基准单点调优 ➔ 宏观复测验收”的工业级闭环；
  - 宿主机与 CI 无需安装任何第三方压测客户端，保证 100% 环境自包含；
  - 规范化度量核心热点算法的算力开销与 GC 压力，根除单测偶发耗时误报；
  - 明确了单机与集群、轻载与峰值的多梯度 SLA 预期，杜绝脱离硬件物理排队极限的虚假断言。
- **代价与边界**：
  - 本地运行 k6 压测需要先通过 Docker 启动依赖容器与后端服务；
  - 阶段一压测聚焦单机并发与核心读链路（`/feed/listLatest`），全站分布式发压与写链路事务压测留作后续演进。
