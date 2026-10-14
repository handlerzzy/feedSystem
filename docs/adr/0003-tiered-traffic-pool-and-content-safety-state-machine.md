# ADR-0003: 阶梯流量池冷启动赛马引擎与合规机审状态机架构设计

## 状态
Accepted

## 背景与问题 (Context)
在完成向量语义召回与性能基准体系重构后，FeedFlow 仍面临两个核心业务与工程痛点：
1. **内容生态马太效应与冷启动匮乏**：原推荐流仅按全局热度或朴素时间序下发，新发布视频缺乏受控的试水流量，长尾优质创作者作品由于缺乏初始反馈直接沉底（Cold Start Starvation）；
2. **合规安全与状态机缺失**：原发布链路缺乏机审前置拦截，任何违规内容直接持久化并流入公域推荐；
3. **高并发赛马陷阱与算法漏洞**：若采用朴素的“下发即曝光”和“固定 200 曝光阈值”，极易遭遇恶意爬虫刷量误伤（攻击者刷曝光稀释互动率致使优质视频沉底）、长尾视频配额不足永久死锁（饿死在种子池）、以及后台定时扫 MySQL 导致的慢查询雪崩与多 Worker 重复推送 SSE 喜报。

## 架构决策 (Decision)

### 1. L1 内存同步轻机审阻断 + 合规状态机
- **L1 同步前缀树阻断**：在 `VideoService.Publish` 核心事务前置执行基于内存 Trie 树的敏感词检测（耗时 <0.2ms），命中违规词直接返回 `apierror.ErrContentViolation`（HTTP 400 阻断），杜绝轻量机审异步化引入的无谓轮询与死信补偿开销；
- **状态机与读隔离**：视频实体新增 `status`（`auditing` / `published` / `rejected`）、`pool_level`（0 沉底池 / 1 种子池 / 2 进阶推荐池）、`reject_reason`。公域 Feed 读链路（`listLatest`, `listByPopularity`, `listByTag`）强制追加 `status = 'published'` 隔离。

### 2. Outbox 多事件路由隔离与延时扇出
- 修复 `outboxworker.go` 现存的无差别抢跑缺陷：在 `claim()` 抢占与 `deliver()` 投递中明确基于 `event_type = 'timeline'` 进行消息过滤；
- 严格保证仅在视频合规发布后，才向 `outbox_msgs` 写入时间线事件，彻底杜绝违规内容抢先扇出至关注者 Feed。

### 3. 单用户防重赛马曝光 (Racing Impressions) 与 Redis ZSet 候选池
- **领域概念解耦**：严格区分用户级“曝光过滤 (Exposed Filter)”与视频级“赛马曝光量 (Racing Impressions)”；
- **单用户滑动防重计数**：在 `internal/feed/racing_impressions.go` 中，利用 Redis Set 记录 `feed:video:racing_users:{video_id}`（TTL 24小时）。单用户对同一视频在时间窗内至多计入 1 次有效赛马曝光，杜绝单客户端连续刷新或恶意爬虫虚假刷量；
- **ZSet 活跃队列防慢查询**：视频产生赛马曝光时，同步通过 `ZINCRBY feed:racing:candidates` 维护活跃候选池（Score 为当前曝光数），避免后台 Worker 定期全表扫描 MySQL `WHERE pool_level = 1`。

### 4. 双触发赛马结算与平滑互动评分公式
- **双触发结算机制**：
  - *触发条件 A (曝光达标)*：累计有效赛马曝光 $\ge 200$；
  - *触发条件 B (时间兜底)*：发布时间超过 24 小时且曝光 $\ge 50$；若满 24 小时曝光仍不足 50，则判定为低潜内容温和沉底（`pool_level = 0`），彻底根治长尾视频死锁饿死。
- **贝叶斯/Laplace 平滑评分公式**：
  $$\text{Score} = \frac{\text{Likes} \times 2 + \text{Comments} \times 3 + 1}{\text{Impressions} + 20}$$
  引入平滑先验因子，解决 $E \approx 200$ 极端小样本下偶发 1 条评论即造成评分失真（摇摆 1.5%）的统计缺陷。
- **跃迁阈值**：
  - $\text{Score} \ge 3\%$：跃迁至 `pool_level = 2`（进阶热门推荐池）；
  - $\text{Score} < 0.8\%$：降级至 `pool_level = 0`（沉底退出探索）。

### 5. MySQL CAS 乐观锁跃迁与 SSE 幂等推送
- Worker 执行状态跃迁时强制采用 CAS 条件更新：
  `UPDATE videos SET pool_level = ?, update_time = NOW() WHERE id = ? AND pool_level = 1`；
- 仅当影响行数 `RowsAffected == 1` 时，才调用 `SSEHub` 向作者实时广播“作品晋升进阶池”喜报，天然防范多实例 Worker 竞态与重复弹窗骚扰。

### 6. 大厂面试防伪与 A/B Test 评估指标闭环
- **A/B Test 分桶实验方案**：基于 `AuthorID % 100` 实施正交分桶（对照组：传统无赛马基准流；实验组：阶梯流量池赛马机制）；
- **核心评估指标体系**：
  1. **冷启动千次曝光互动增益 (E-RPM)**：$\text{E-RPM} = \frac{\text{Total Interactions}}{\text{Total Impressions}} \times 1000$（实验组预期提升 15%+）；
  2. **新视频 72 小时赛马通关率**：从 Level 1 晋升至 Level 2 的良品比例（健康区间通常在 8% ~ 15%）；
  3. **内容消费基尼系数 (Gini Coefficient)**：监控全平台前 5% 头部视频消耗的总流量占比，防止赛马机制沦为极端马太效应。

## 权衡与影响 (Consequences)
- **优势**：
  - 业务模型彻底摆脱简单 CRUD 玩具刻板印象，高度契合字节跳动/快手核心分发生态；
  - 核心结算链路基于 Redis ZSet 与 MySQL CAS，完全免疫高并发重复跃迁与慢查询雪崩；
  - 面试叙事逻辑环环相扣，从底层并发安全到上层推荐指标闭环均有完备理论支撑。
- **代价与演进边界**：
  - 内存 Trie 树适用于千万级以下文本词库检测；若未来需引入图像涉黄、多模态音频质检或大模型深度审核，需平滑演进为 L2 异步分析旁路。
