package feed

import (
	"math"
	"sort"
	"time"

	"github.com/handlerzzy/feedsystem/internal/video"
)

// 本文件实现 Ticket 05「本地轻量多特征精排与彻底剥离外部大模型 RPC」。
//
// 架构决策 (ADR-0001 §5 / Handbook Q6)：
// 在线读链路要求 P99 < 8ms 的极高响应速度与高可用性，严禁在读链路上同步调用外部大语言模型（LLM）或 Sidecar RPC。
// 精排在 Go 进程内部通过轻量多特征综合公式（语义相似度 + 时间指数衰减 + 互动热度）在纯内存中执行微秒级极速打分与重排：
//
//    Score = w1 * 余弦相似度 + w2 * 时间指数衰减 + w3 * log(点赞热度 + 1)
//
// 纯内存打分耗时低于 0.1ms，兼顾相关性、时效性与冷启动互动多样性。

// 本地轻量精排默认权重与参数配置
const (
	DefaultWeightSemantic   = 0.6 // w1: 语义余弦相似度权重
	DefaultWeightTimeDecay  = 0.3 // w2: 时间指数衰减权重
	DefaultWeightPopularity = 0.1 // w3: 互动/点赞热度权重

	// DefaultTimeDecayHalfLifeHours 时间衰减半衰期（默认 72 小时 / 3 天）。
	// 3 天前发布的视频时间分衰减为 0.5，6 天前衰减为 0.25。
	DefaultTimeDecayHalfLifeHours = 72.0
)

// LocalRerankOptions 本地轻量精排参数。
type LocalRerankOptions struct {
	WeightSemantic   float64
	WeightTimeDecay  float64
	WeightPopularity float64
	HalfLifeHours    float64
}

// DefaultLocalRerankOptions 返回默认精排配置。
func DefaultLocalRerankOptions() LocalRerankOptions {
	return LocalRerankOptions{
		WeightSemantic:   DefaultWeightSemantic,
		WeightTimeDecay:  DefaultWeightTimeDecay,
		WeightPopularity: DefaultWeightPopularity,
		HalfLifeHours:    DefaultTimeDecayHalfLifeHours,
	}
}

// ScoredFeedCandidate 带有多特征综合得分的候选视频。
type ScoredFeedCandidate struct {
	Video       *video.Video
	SemanticSim float64 // 语义相似度 [0, 1]
	TimeScore   float64 // 时间指数衰减 [0, 1]
	HeatScore   float64 // log(点赞热度 + 1) 归一化得分
	FinalScore  float64 // 多特征综合打分
}

// CalculateLocalRerankScore 计算单条视频的本地综合特征打分。
// 公式：Score = w1 * 余弦相似度 + w2 * 时间指数衰减 + w3 * log(点赞热度 + 1)。
func CalculateLocalRerankScore(v *video.Video, semanticSim float64, now time.Time, opts LocalRerankOptions) ScoredFeedCandidate {
	if v == nil {
		return ScoredFeedCandidate{}
	}

	// 1. 语义相似度特征 (范围限制在 [0, 1])
	sim := semanticSim
	if sim < 0 {
		sim = 0
	} else if sim > 1 {
		sim = 1
	}

	// 2. 时间指数衰减特征：decay = 2^(-Δt / halfLife)
	halfLife := opts.HalfLifeHours
	if halfLife <= 0 {
		halfLife = DefaultTimeDecayHalfLifeHours
	}
	diffHours := 0.0
	if !v.CreateTime.IsZero() && now.After(v.CreateTime) {
		diffHours = now.Sub(v.CreateTime).Hours()
	}
	timeScore := math.Pow(2, -diffHours/halfLife)
	if math.IsNaN(timeScore) || math.IsInf(timeScore, 0) || timeScore < 0 {
		timeScore = 0
	} else if timeScore > 1 {
		timeScore = 1
	}

	// 3. 互动/点赞热度特征：使用 log(LikesCount + 1)，上限平滑至 10,000 赞
	likes := v.LikesCount
	if likes < 0 {
		likes = 0
	}
	// log1p(10000) ≈ 9.2103
	heatScore := math.Log1p(float64(likes)) / math.Log1p(10000.0)
	if heatScore > 1.0 {
		heatScore = 1.0
	} else if heatScore < 0 {
		heatScore = 0
	}

	// 4. 加权综合得分
	w1 := opts.WeightSemantic
	w2 := opts.WeightTimeDecay
	w3 := opts.WeightPopularity
	finalScore := w1*sim + w2*timeScore + w3*heatScore

	return ScoredFeedCandidate{
		Video:       v,
		SemanticSim: sim,
		TimeScore:   timeScore,
		HeatScore:   heatScore,
		FinalScore:  finalScore,
	}
}

// compareLocalRerankCandidate 比较两个本地打分候选的排序优先级（降序排序）。
// 规则：综合得分高者优先；同分发布时间更晚优先；同时间 ID 降序（严格可复现）。
func compareLocalRerankCandidate(aFinalScore float64, aVideo *video.Video, bFinalScore float64, bVideo *video.Video) int {
	if math.Abs(aFinalScore-bFinalScore) > 1e-9 {
		if aFinalScore > bFinalScore {
			return -1
		}
		return 1
	}
	if aVideo != nil && bVideo != nil {
		if !aVideo.CreateTime.Equal(bVideo.CreateTime) {
			if aVideo.CreateTime.After(bVideo.CreateTime) {
				return -1
			}
			return 1
		}
		if aVideo.ID > bVideo.ID {
			return -1
		} else if aVideo.ID < bVideo.ID {
			return 1
		}
	}
	return 0
}

// LocalRerankVideos 对候选视频列表执行纯本地极速多特征重排（耗时 <0.1ms）。
func LocalRerankVideos(items []*video.Video, semanticScores map[uint]float64, now time.Time, opts LocalRerankOptions) []*video.Video {
	if len(items) < 2 {
		return items
	}
	scored := make([]ScoredFeedCandidate, len(items))
	for i, v := range items {
		sim := 0.0
		if semanticScores != nil {
			sim = semanticScores[v.ID]
		}
		scored[i] = CalculateLocalRerankScore(v, sim, now, opts)
	}

	// 按综合得分降序排序；同分按发布时间倒序；再同分按 ID 倒序（严格可复现）
	sort.SliceStable(scored, func(i, j int) bool {
		return compareLocalRerankCandidate(scored[i].FinalScore, scored[i].Video, scored[j].FinalScore, scored[j].Video) < 0
	})

	out := make([]*video.Video, len(items))
	for i, s := range scored {
		out[i] = s.Video
	}
	return out
}

// LocalRerankSemanticHits 对语义召回候选列表应用本地轻量多特征打分与重排。
// 结合余弦相似度、时间指数衰减与点赞热度，选出综合质量最高的候选，
// 随后输入 fuseChannels 进行配额插槽，严格捍卫配额槽位融合领域模型（ADR-0001 §5 / Task 3 方案 A）。
func LocalRerankSemanticHits(hits []SemanticHit, now time.Time, opts LocalRerankOptions) []SemanticHit {
	if len(hits) < 2 {
		return hits
	}
	type scoredHit struct {
		hit   SemanticHit
		score ScoredFeedCandidate
	}
	scored := make([]scoredHit, len(hits))
	for i, h := range hits {
		scored[i] = scoredHit{
			hit:   h,
			score: CalculateLocalRerankScore(h.Video, h.Score, now, opts),
		}
	}

	sort.SliceStable(scored, func(i, j int) bool {
		return compareLocalRerankCandidate(scored[i].score.FinalScore, scored[i].hit.Video, scored[j].score.FinalScore, scored[j].hit.Video) < 0
	})

	out := make([]SemanticHit, len(hits))
	for i, s := range scored {
		out[i] = s.hit
	}
	return out
}
