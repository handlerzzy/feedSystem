package feed

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/handlerzzy/feedsystem/internal/logging"
	"github.com/handlerzzy/feedsystem/internal/video"

	"go.uber.org/zap"
)

// 本文件实现 P2 §4.2「向量召回通道」。
//
// 接口（VectorRecallStore）声明在 feed_store.go —— 消费方声明，实现方满足，
// 与本项目既有惯例一致。这里放的是**消费方这一侧**的逻辑：
// 用户兴趣向量怎么来、候选池怎么圈、过滤与去重怎么做、相似度怎么算。
//
// 为什么检索这一层刻意不进仓储包（P2 前置 B4 的决定）：
// 检索是"把候选向量读进内存再算余弦"，是一个纯计算 + 一次批量读。
// 把它放进仓储层，会让最需要单测覆盖的部分（过滤、去重、排序）
// 必须连上真库才能验证。

// SemanticQuota 相关的常量。
const (
	// SemanticCandidateWindow 是一次语义召回最多考虑多少条**有向量的**视频。
	//
	// 它是 P2 前置 B4「应用内暴力检索」能成立的硬前提：没有它，
	// 向量池一大，每次 Feed 请求都会把整个池子读进内存。
	// 依据与升级触发条件见 AI-P2-设计决定.md 第 1 节
	// （单次扫描量 > 5,000 条或召回段 p99 > 20ms 就必须换 ANN）。
	//
	// 取 500 而不是 200：D4 的 200 是**融合后的候选池**，而语义路需要
	// 一个比它更大的搜索空间，否则"老但相关"的内容在一开始就被截掉了。
	// 500 × 1536 维 float32 ≈ 3MB 内存、一次全量点积约 0.4 MFLOP，
	// 在 D1 的 50ms 预算里只占很小一段（实测见 README 的延迟表）。
	SemanticCandidateWindow = 500

	// SemanticWarmupExclude 是做用户画像时**排除**掉的最新视频条数。
	//
	// 为什么必须排除：最新的一批内容时序路已经保证了它们的曝光，
	// 而且它们往往是"平台上最新的热词"，会被画像当成用户兴趣，
	// 于是语义位里塞满刚发布的视频，与探索配额、时序路三重重复。
	//
	// 取 20（一页）而不是"全部"：排除太多会把真正有用的近期兴趣也排掉，
	// 那时画像只剩很久以前的偏好，反而让语义路变钝。
	SemanticWarmupExclude = 20

	// UserProfileLikes 是构建用户兴趣向量时最多取多少条最近点赞。
	//
	// 上限的意义：兴趣是**会变**的，把三年前的点赞和昨天的等权平均，
	// 画像会停在一个"平均人"上。取 50 条是"最近偏好"与"样本足够稳"
	// 之间的折中（再少则单条噪声大的行为会主导画像）。
	UserProfileLikes = 50

	// SemanticMaxCandidates 是一次召回最多返回多少个候选。
	//
	// 它只需要覆盖"融合阶段可能用到的语义位"，取 3×SemanticCandidateWindow/10
	// 太大也没有意义：真正进入最终列表的只有 recall_quota × limit 条。
	// 留出余量是为了让融合阶段在去重（与基础页重复）之后还有得选。
	SemanticMaxCandidates = 50

	// LongTailMaxCandidates 是两级召回中长尾标签粗筛的最大候选条数（50~100 条）。
	// 与常驻内存池的 2,000 条活跃视频合并打分，兼顾时效热度与长尾覆盖。
	LongTailMaxCandidates = 100

	// LongTailTopTags 是分析用户近期点赞偏好时提取的高频标签数量。
	LongTailTopTags = 3
)

// SemanticHit 是一条语义召回结果。
type SemanticHit struct {
	Video *video.Video
	// Score 是用户兴趣向量与该视频向量的余弦相似度，范围 [-1, 1]。
	Score float64
}

// semanticRecall 执行一次语义召回。任何失败都返回空结果 + nil 错误。
//
// 为什么**不向上返回错误**：语义路是可缺省的一层（P2 §4.1 的硬要求）。
// 向量表被删、某个 model 的向量还没回填、Redis/DB 抖动——这些都不该
// 让一次 Feed 请求变成 5xx，也不该让调用方去区分"哪种失败"。
// 失败信息通过日志暴露，调用方只看到"这一路这次没有产出"。
//
// 用户维度缺失时（viewerAccountID == 0，即未登录）直接返回空：
// 没有用户就没有兴趣向量，全局兴趣向量会让"个性化"变成一个谎言
// （那只是热榜的另一种算法）。未登录用户退回原有的时序/热榜排序，
// 正是 P2 §4.5 对"新用户"的要求。
func (f *FeedService) semanticRecall(ctx context.Context, viewerAccountID uint, quota QuotaPlan) []SemanticHit {
	if f.vectorStore == nil || f.vectorPool == nil || f.vectorPool.Len() == 0 || quota.Semantic <= 0 || viewerAccountID == 0 {
		return nil
	}
	// 整段共用一个预算：向量读取与相似度计算都在这条路径上，
	// 分开设超时会让总耗时变成两段之和（D1 的 50ms 是**总共**的预算）。
	opCtx, cancel := context.WithTimeout(ctx, f.semanticTimeout)
	defer cancel()

	// 1. 获取用户近期点赞的视频 ID 列表
	likedIDs, err := f.recentLikedVideoIDs(opCtx, viewerAccountID, UserProfileLikes)
	if err != nil {
		f.logSemanticDegrade(ctx, "读取用户点赞历史失败，语义路本次关闭", err)
		return nil
	}
	if len(likedIDs) == 0 {
		// 新用户 / 没有点赞：没有可用的个性化信号。这是 P2 §4.5 明确
		// 描述的冷启动状态，退回原有排序即可，不是故障。
		return nil
	}

	// 2. 基于点赞视频合成用户即时兴趣向量（优先命中常驻内存向量池，未命中时按需 LoadVectors）
	profile, err := f.userInterestVectorFromLikedIDs(opCtx, likedIDs)
	if err != nil {
		f.logSemanticDegrade(ctx, "构建用户兴趣向量失败，语义路本次关闭", err)
		return nil
	}
	if len(profile) == 0 {
		return nil
	}

	// 3. 两级候选圈定机制 (Ticket 05 / ADR-0001 §2)：
	// 第一级：常驻内存向量池（2,000 条活跃视频）
	// 第二级：提取用户近期高频点赞的 Top 标签，通过已有标签倒排索引定向粗筛 50~100 条历史长尾候选并补齐向量
	extraLongTailItems, longTailVideoMap := f.roughSelectLongTailCandidates(opCtx, likedIDs)

	// 4. 纯内存余弦相似度打分与 Top-K 截取（耗时 <2ms）。
	// 在检索前获取最新一批视频 ID 集合用于前置排除（让位给时序路），
	// 以及获取用户近期已看视频曝光集合用于滑动过滤（消除跨页重复推荐）。
	excluded := f.warmupExclusionSet(opCtx)
	exposed := f.getExposedFilterSet(opCtx, viewerAccountID)

	scored := f.vectorPool.SearchCosineWithExtra(profile, extraLongTailItems, SemanticMaxCandidates, func(item video.VectorItem) bool {
		if excluded != nil && excluded[item.VideoID] {
			return false
		}
		if exposed != nil && exposed[item.VideoID] {
			return false
		}
		return true
	})
	if len(scored) == 0 {
		return nil
	}

	// 5. 取回完整视频对象（长尾已粗筛出的直接复用，其余批量 GetByIDs 查询）。
	videoByID := make(map[uint]*video.Video, len(scored))
	var needFetchIDs []uint
	for _, s := range scored {
		id := s.Item.VideoID
		if v, ok := longTailVideoMap[id]; ok && v != nil {
			videoByID[id] = v
		} else {
			needFetchIDs = append(needFetchIDs, id)
		}
	}
	if len(needFetchIDs) > 0 {
		videos, err := f.repo.GetByIDs(opCtx, needFetchIDs)
		if err != nil {
			f.logSemanticDegrade(ctx, "读取召回视频失败，语义路本次关闭", err)
			return nil
		}
		for _, v := range videos {
			videoByID[v.ID] = v
		}
	}

	// 6. 过滤：无效 ID、缺失视频、作者自看/拉黑等业务过滤。顺序保持打分顺序。
	// 注意：excluded（时序最新批次）与 exposed（滑动曝光集合）已在 SearchCosineWithExtra 回调中前置过滤，此处无需二次判断。
	hits := make([]SemanticHit, 0, len(scored))
	for _, s := range scored {
		v := videoByID[s.Item.VideoID]
		if v == nil {
			// 向量池里有、视频表里没有：说明这条视频已被删除（或向量行是孤儿）。
			continue
		}
		if f.candidateFilter != nil && !f.candidateFilter(opCtx, viewerAccountID, v) {
			continue
		}
		hits = append(hits, SemanticHit{Video: v, Score: s.Score})
	}
	if len(hits) == 0 {
		return nil
	}

	// 7. 本地轻量多特征精排（ADR-0001 §5 / Task 3 方案 A）：
	// 本地多特征精排打分前置到语义召回通道内部（对 hits 基于【语义分 + 时间指数衰减 + 互动热度】进行极速特征打分与重排），
	// 随后输入 fuseChannels 进行交错插槽，严格捍卫配额槽位融合规范，防止全量重排导致高分语义视频集中霸屏破坏时序基准流。
	hits = LocalRerankSemanticHits(hits, time.Now(), DefaultLocalRerankOptions())
	return hits
}

// scoredCandidate 是打分阶段的中间结果。
type scoredCandidate struct {
	id        uint
	createdAt time.Time
	score     float64
}

// sortCandidates 按分数降序、同分按 ID 降序。
//
// 为什么必须显式定死同分的次序：map 遍历顺序随机，同分不排序会让同一份
// 数据两次请求返回不同的列表——可复现性是这套东西能被测试的前提
// （与 evalset 里"按 id 降序保证可复现"是同一条纪律）。
// 正相关（score > 0）才保留：余弦为负表示方向相反，把它排进推荐位
// 等于"按用户最不喜欢的主题推荐"，而过零点附近的噪声又会随机换人。
func sortCandidates(items []scoredCandidate) {
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.score != b.score {
			return a.score > b.score
		}
		if !a.createdAt.Equal(b.createdAt) {
			return a.createdAt.After(b.createdAt)
		}
		return a.id > b.id
	})
}

// warmupExclusionSet 返回"最新的一批视频 ID"，语义路会把它们让给时序路。
//
// 失败时返回空集合而不是错误：排除集是**优化**而不是正确性前提，
// 拿不到它最多是语义位里多几条新视频，不该因此关掉整条语义路。
func (f *FeedService) warmupExclusionSet(ctx context.Context) map[uint]bool {
	fresh, ok := f.repo.(FreshIDLister)
	if !ok {
		return nil
	}
	ids, err := fresh.NewestVideoIDs(ctx, SemanticWarmupExclude)
	if err != nil {
		logging.Ctx(ctx).Warn("读取最新视频 ID 失败，语义路不再排除新内容（不影响 Feed 结果）",
			zap.Error(err))
		return nil
	}
	out := make(map[uint]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out
}

// userInterestVector 由用户最近点赞的视频向量加权平均得到。
//
// 权重：越近的点赞权重越高（线性衰减 1.0 → 1/n）。
// 为什么不是等权：兴趣是会漂的，等权会让"三年前点赞过 50 条宠物视频"
// 永远压过"昨天点赞了 3 条 Go 视频"，而后者才是当前画像。
// 为什么不是指数衰减：指数衰减下最近一条几乎独裁画像，单条行为的噪声
// （误点、好奇点开）会被放大成整个兴趣向量。
//
// 返回零长切片表示"没有可用的兴趣向量"（没有点赞、或点赞的视频都没有向量）。
func (f *FeedService) userInterestVector(ctx context.Context, accountID uint) ([]float32, error) {
	if f.vectorStore == nil {
		return nil, nil
	}
	likedIDs, err := f.recentLikedVideoIDs(ctx, accountID, UserProfileLikes)
	if err != nil {
		return nil, err
	}
	return f.userInterestVectorFromLikedIDs(ctx, likedIDs)
}

// userInterestVectorFromLikedIDs 基于给定的点赞视频 ID 列表加权平均合成用户即时兴趣向量并做 L2 归一化。
func (f *FeedService) userInterestVectorFromLikedIDs(ctx context.Context, likedIDs []uint) ([]float32, error) {
	if len(likedIDs) == 0 {
		return nil, nil
	}

	dim := f.embeddingDim
	vectors := make(map[uint][]float32, len(likedIDs))
	var missingIDs []uint

	// 优先在常驻内存向量池中命中点赞视频的向量（零 IO），仅当内存池未命中时才回退查库
	for _, id := range likedIDs {
		if f.vectorPool != nil {
			if item, ok := f.vectorPool.Get(id); ok && len(item.Vector) == dim {
				vectors[id] = item.Vector
				continue
			}
		}
		missingIDs = append(missingIDs, id)
	}

	if len(missingIDs) > 0 && f.vectorStore != nil {
		loaded, err := f.vectorStore.LoadVectors(ctx, missingIDs, f.effectiveModel(ctx), dim)
		if err != nil {
			return nil, err
		}
		for id, vec := range loaded {
			if len(vec) == dim {
				vectors[id] = vec
			}
		}
	}

	if len(vectors) == 0 {
		// 点赞过的视频还没有向量（回填没跑到）：语义路无信号。
		return nil, nil
	}

	// 维度在累加前定死：不同维度的向量相加会在 Go 里 panic
	// （索引越界），而"少数几条维度不对"是完全可能的历史数据。
	acc := make([]float64, dim)
	var total float64
	for rank, id := range likedIDs {
		vec := vectors[id]
		if len(vec) != dim {
			continue
		}
		weight := 1 / float64(rank+1)
		for i, v := range vec {
			acc[i] += float64(v) * weight
		}
		total += weight
	}
	if total == 0 {
		return nil, nil
	}
	out := make([]float32, dim)
	for i := range acc {
		out[i] = float32(acc[i] / total)
	}
	return video.L2Normalize(out), nil
}

// roughSelectLongTailCandidates 分析用户近期高频点赞的 Top 标签，通过已有标签索引定向粗筛 50~100 条历史长尾候选并补齐向量。
func (f *FeedService) roughSelectLongTailCandidates(ctx context.Context, likedIDs []uint) ([]video.VectorItem, map[uint]*video.Video) {
	if len(likedIDs) == 0 || f.repo == nil {
		return nil, nil
	}

	// 1. 获取点赞视频的标签列表
	tagMap, err := f.videoTagNames(ctx, likedIDs)
	if err != nil || len(tagMap) == 0 {
		return nil, nil
	}

	// 2. 统计用户高频点赞标签
	tagCounts := make(map[string]int)
	for _, tags := range tagMap {
		seen := make(map[string]bool, len(tags))
		for _, tag := range tags {
			if tag != "" && !seen[tag] {
				seen[tag] = true
				tagCounts[tag]++
			}
		}
	}
	if len(tagCounts) == 0 {
		return nil, nil
	}

	type tagFreq struct {
		name  string
		count int
	}
	sortedTags := make([]tagFreq, 0, len(tagCounts))
	for name, count := range tagCounts {
		sortedTags = append(sortedTags, tagFreq{name: name, count: count})
	}
	sort.Slice(sortedTags, func(i, j int) bool {
		if sortedTags[i].count != sortedTags[j].count {
			return sortedTags[i].count > sortedTags[j].count
		}
		return sortedTags[i].name < sortedTags[j].name
	})

	topN := LongTailTopTags
	if len(sortedTags) < topN {
		topN = len(sortedTags)
	}
	topTags := sortedTags[:topN]

	// 3. 通过已有标签倒排索引粗筛 50~100 条历史长尾候选
	perTagLimit := LongTailMaxCandidates / len(topTags)
	if perTagLimit < 30 {
		perTagLimit = 30
	}

	// 排除当前常驻内存向量池中已有的视频 ID，下潜捞取真正的历史长尾内容（Task 9 / P2）。
	var poolExcludeIDs []uint
	if f.vectorPool != nil {
		poolExcludeIDs = f.vectorPool.VideoIDs()
	}

	longTailVideos := make(map[uint]*video.Video)
	longTailLister, hasLongTail := f.repo.(LongTailTagLister)
	for _, tf := range topTags {
		var candidates []*video.Video
		if hasLongTail {
			candidates, err = longTailLister.ListLongTailByTag(ctx, tf.name, perTagLimit, poolExcludeIDs)
		} else {
			candidates, err = f.repo.ListByTag(ctx, tf.name, perTagLimit)
		}
		if err != nil {
			continue
		}
		for _, v := range candidates {
			if v != nil && len(longTailVideos) < LongTailMaxCandidates {
				longTailVideos[v.ID] = v
			}
		}
		if len(longTailVideos) >= LongTailMaxCandidates {
			break
		}
	}

	if len(longTailVideos) == 0 {
		return nil, nil
	}

	// 4. 补齐向量：优先从内存向量池命中，未命中则批量从持久化仓储 LoadVectors
	var missingVectorIDs []uint
	extraItems := make([]video.VectorItem, 0, len(longTailVideos))
	for id := range longTailVideos {
		if f.vectorPool != nil {
			if item, ok := f.vectorPool.Get(id); ok && len(item.Vector) == f.embeddingDim {
				extraItems = append(extraItems, item)
				continue
			}
		}
		missingVectorIDs = append(missingVectorIDs, id)
	}

	if len(missingVectorIDs) > 0 && f.vectorStore != nil {
		loaded, err := f.vectorStore.LoadVectors(ctx, missingVectorIDs, f.effectiveModel(ctx), f.embeddingDim)
		if err == nil {
			for id, vec := range loaded {
				if len(vec) == f.embeddingDim {
					if v := longTailVideos[id]; v != nil {
						extraItems = append(extraItems, video.VectorItem{
							VideoID:    id,
							CreateTime: v.CreateTime,
							Vector:     vec,
						})
					}
				}
			}
		}
	}

	return extraItems, longTailVideos
}

// cosine 计算两个向量的余弦相似度。
//
// 长度不一致时按较短的一侧计算并返回 0 之外的**未定义**结果——这里选择
// 直接返回 0：维度不一致说明两边不在同一个向量空间，任何数值都没有意义，
// 而返回 0 会让这条候选自然沉底（而不是被 panic 打断整次请求）。
func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na <= 0 || nb <= 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// logSemanticDegrade 记录一次语义路降级。
//
// 用 Info 而不是 Warn：语义路可缺省是**设计**，不是异常。
// "向量还没回填完"在新部署上会持续出现，用 Warn 会让它在告警里
// 与真正的故障无法区分（与时间线补齐那条日志同一个理由）。
// 需要人介入的情形是"持续降级"，那要看的是这条日志的**频率**。
func (f *FeedService) logSemanticDegrade(ctx context.Context, msg string, err error) {
	logging.Ctx(ctx).Info(msg,
		zap.String("model", f.embeddingModel),
		zap.Int("dim", f.embeddingDim),
		zap.Error(err))
}

// embeddingModelCacheTTL 是"当前在用的向量空间名"这个结论的缓存时长。
//
// 它是一次 `GROUP BY model` 查询（向量表在 (model,dim) 上有索引，
// 但 GROUP BY 仍然要扫一批行），因此**每个请求都查一次是不可接受的**：
// 那等于把"避免打库"这件事取消了。
//
// 30 秒的依据：换模型（含从真实 provider 切到离线演示）是一个运维动作，
// 半分钟内生效完全够用；而超过这个窗口还读不到向量时，
// semanticRecall 会主动失效缓存再解析一次（见该函数），
// 因此"换模型后语义路静默失效"最多持续到下一次请求。
const embeddingModelCacheTTL = 30 * time.Second

// embeddingModelCacheKey 是缓存键。用常量而不是拼 embeddingModel：
// 缓存的内容是"库里实际是什么"，与配置值无关（配置值只在没有任何
// 已入库向量时作为兜底）。
const embeddingModelCacheKey = "feed:embedding_model:effective"

// effectiveModel 返回**读取向量时应该使用的模型名**。
//
// 背景（一个真实会踩的坑）：向量的 model 由提供方决定，不是配置决定。
// 离线演示时 sidecar 用词法编码器，它回的是 faux-lexical，而配置里写的是
// text-embedding-3-small。若按配置名去读，一条向量都读不到，
// 表现为"语义路静默失效"——而日志里一切正常。
//
// 解析顺序：
//
//  1. 缓存（30 秒）；
//  2. 库里最近写入的那个向量空间（这才是"现在正在生产的那批"）；
//  3. 库里没有任何向量时退回配置值——此时语义路本来就无数据可用，
//     退回配置值只是为了让日志里的 model 字段可读。
//
// 解析失败按"没有向量"处理并返回配置值：语义路是可缺省的一层，
// 它不该因为一次统计查询失败而报错，也不该让调用方去区分失败类型。
func (f *FeedService) effectiveModel(ctx context.Context) string {
	if f.vectorStore == nil {
		return f.embeddingModel
	}
	if f.modelCache != nil {
		if v, found := f.modelCache.Get(embeddingModelCacheKey); found {
			if name, ok := v.(string); ok && name != "" {
				return name
			}
		}
	}
	models, err := f.vectorStore.ListEmbeddingModels(ctx, f.embeddingDim, 4)
	if err != nil {
		logging.Ctx(ctx).Info("读取向量空间失败，本次按配置里的模型名尝试（不影响 Feed 结果）",
			zap.String("configured_model", f.embeddingModel), zap.Error(err))
		return f.embeddingModel
	}
	name := f.embeddingModel
	for _, m := range models {
		if m.Rows > 0 && m.Model != "" {
			name = m.Model
			break
		}
	}
	if name != f.embeddingModel {
		// 只在两者不同时记日志：正常情况下（配置名就是库里的名字）
		// 每个进程启动后记一条没有信息量的日志只是噪音。
		logging.Ctx(ctx).Info("语义路使用的向量空间与配置的模型名不同（按库里的实际空间读取）",
			zap.String("configured_model", f.embeddingModel),
			zap.String("effective_model", name),
			zap.Int("dim", f.embeddingDim))
	}
	if f.modelCache != nil {
		f.modelCache.Set(embeddingModelCacheKey, name, embeddingModelCacheTTL)
	}
	return name
}

// invalidateEffectiveModel 清掉模型解析缓存。
//
// 什么时候需要它：一次读取返回了 0 条候选时，最可能的原因就是
// "库里的向量空间变了"（刚跑完一轮换模型后的回填）。
// 不主动失效的话，语义路会一直用旧名字读到 TTL 结束——表现为
// "换完模型之后要等半分钟才好"。
func (f *FeedService) invalidateEffectiveModel() {
	if f.modelCache != nil {
		f.modelCache.Delete(embeddingModelCacheKey)
	}
}
