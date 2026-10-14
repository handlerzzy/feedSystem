package worker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/handlerzzy/feedsystem/internal/logging"
	rediscache "github.com/handlerzzy/feedsystem/internal/middleware/redis"
	"github.com/handlerzzy/feedsystem/internal/video"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// DefaultTrafficPoolPollInterval 是赛马结算调度器的默认轮询间隔（5 秒）。
	DefaultTrafficPoolPollInterval = 5 * time.Second

	// DefaultTrafficPoolBatchLimit 是单批次候选处理的默认上限。
	DefaultTrafficPoolBatchLimit = 100

	// DefaultRacingExposureThreshold 是触发赛马结算的有效曝光阈值（200 次）。
	DefaultRacingExposureThreshold int64 = 200

	// DefaultDeadlineExposureMin 是 24 小时兜底结算时认定为有潜力的最低曝光门槛（50 次）。
	DefaultDeadlineExposureMin int64 = 50

	// DefaultDeadlineDuration 是赛马生存周期的最大兜底时间窗口（24 小时）。
	DefaultDeadlineDuration = 24 * time.Hour

	// DefaultPromotionScoreThreshold 是晋升进阶池（pool_level = 2）的最低平滑互动分（3%）。
	DefaultPromotionScoreThreshold = 0.03

	// DefaultSinkScoreThreshold 是沉底淘汰（pool_level = 0）的平滑互动分门槛（0.8%）。
	DefaultSinkScoreThreshold = 0.008

	// RacingCandidatesKey 是活跃赛马候选队列的 Redis ZSet 键名。
	RacingCandidatesKey = "feed:racing:candidates"
)

// CalculateLaplaceScore 依据 Laplace 平滑互动公式计算视频质量互动得分：
// Score = (2L + 3C + 1) / (E + 20)
// 其中 L 为点赞数，C 为评论数，E 为累计有效赛马曝光。
// 分子加 1、分母加 20 作为先验平滑因子，防范极端小样本下的指标剧烈波动。
func CalculateLaplaceScore(likes, comments, impressions int64) float64 {
	if likes < 0 {
		likes = 0
	}
	if comments < 0 {
		comments = 0
	}
	if impressions < 0 {
		impressions = 0
	}
	numerator := float64(likes*2 + comments*3 + 1)
	denominator := float64(impressions + 20)
	return numerator / denominator
}

// FormatPromotionMessage 生成标准格式的赛马晋升喜报文本。
func FormatPromotionMessage(title string, videoID uint) string {
	title = strings.TrimSpace(title)
	if title != "" {
		return fmt.Sprintf("恭喜！您的作品《%s》已晋升至进阶推荐池，正获得更大流量曝光！", title)
	}
	return fmt.Sprintf("恭喜！您的作品 (ID: %d) 已晋升至进阶推荐池，正获得更大流量曝光！", videoID)
}

// SettlementResult 记录单个候选视频赛马结算的裁决结果与执行状态。
type SettlementResult struct {
	VideoID         uint            `json:"video_id"`
	AuthorID        uint            `json:"author_id"`
	Impressions     int64           `json:"impressions"`
	Likes           int64           `json:"likes"`
	Comments        int64           `json:"comments"`
	Score           float64         `json:"score"`
	TargetPoolLevel video.PoolLevel `json:"target_pool_level"`
	Promoted        bool            `json:"promoted"`
	CasSucceeded    bool            `json:"cas_succeeded"`
	Reason          string          `json:"reason"`
}

// TrafficPoolWorker 是阶梯流量池赛马调度引擎，负责定期巡检活跃候选队列并执行双触发结算。
type TrafficPoolWorker struct {
	db                      *gorm.DB
	cache                   *rediscache.Client
	sseHub                  SSEBroadcaster
	interval                time.Duration
	batchLimit              int64
	racingExposureThreshold int64
	deadlineExposureMin     int64
	deadlineDuration        time.Duration
	promotionScoreThreshold float64
	sinkScoreThreshold      float64
	nowFunc                 func() time.Time
	sweepCursor             uint
}

// NewTrafficPoolWorker 创建并初始化赛马调度引擎实例。
func NewTrafficPoolWorker(db *gorm.DB, cache *rediscache.Client, sseHub SSEBroadcaster) *TrafficPoolWorker {
	return &TrafficPoolWorker{
		db:                      db,
		cache:                   cache,
		sseHub:                  sseHub,
		interval:                DefaultTrafficPoolPollInterval,
		batchLimit:              DefaultTrafficPoolBatchLimit,
		racingExposureThreshold: DefaultRacingExposureThreshold,
		deadlineExposureMin:     DefaultDeadlineExposureMin,
		deadlineDuration:        DefaultDeadlineDuration,
		promotionScoreThreshold: DefaultPromotionScoreThreshold,
		sinkScoreThreshold:      DefaultSinkScoreThreshold,
		nowFunc:                 time.Now,
	}
}

// WithInterval 设置轮询检查周期。
func (w *TrafficPoolWorker) WithInterval(d time.Duration) *TrafficPoolWorker {
	if d > 0 {
		w.interval = d
	}
	return w
}

// WithBatchLimit 设置单批次处理候选数量上限。
func (w *TrafficPoolWorker) WithBatchLimit(limit int64) *TrafficPoolWorker {
	if limit > 0 {
		w.batchLimit = limit
	}
	return w
}

// WithClock 注入可控时钟函数，用于确定性时间单测。
func (w *TrafficPoolWorker) WithClock(fn func() time.Time) *TrafficPoolWorker {
	if fn != nil {
		w.nowFunc = fn
	}
	return w
}

// WithThresholds 设置晋升分、沉底分与曝光阈值。
func (w *TrafficPoolWorker) WithThresholds(promoScore, sinkScore float64, racingExposure, deadlineExposure int64) *TrafficPoolWorker {
	if promoScore > 0 {
		w.promotionScoreThreshold = promoScore
	}
	if sinkScore > 0 {
		w.sinkScoreThreshold = sinkScore
	}
	if racingExposure > 0 {
		w.racingExposureThreshold = racingExposure
	}
	if deadlineExposure > 0 {
		w.deadlineExposureMin = deadlineExposure
	}
	return w
}

// WithDeadlineDuration 设置生存周期兜底时间窗口。
func (w *TrafficPoolWorker) WithDeadlineDuration(d time.Duration) *TrafficPoolWorker {
	if d > 0 {
		w.deadlineDuration = d
	}
	return w
}

func (w *TrafficPoolWorker) now() time.Time {
	if w.nowFunc != nil {
		return w.nowFunc()
	}
	return time.Now()
}

func (w *TrafficPoolWorker) candidatesKey() string {
	if w.cache == nil {
		return RacingCandidatesKey
	}
	return w.cache.Key(RacingCandidatesKey)
}

func (w *TrafficPoolWorker) removeCandidate(ctx context.Context, videoID uint) {
	if w.cache == nil || videoID == 0 {
		return
	}
	_ = w.cache.ZRem(ctx, w.candidatesKey(), strconv.FormatUint(uint64(videoID), 10))
}

// executeCASUpdate 采用 GORM 执行 CAS 乐观更新:
// UPDATE videos SET pool_level = ?, update_time = ? WHERE id = ? AND pool_level = 1
func (w *TrafficPoolWorker) executeCASUpdate(ctx context.Context, videoID uint, targetPoolLevel video.PoolLevel) (bool, error) {
	if w.db == nil {
		return false, errors.New("db not initialized")
	}
	res := w.db.WithContext(ctx).Model(&video.Video{}).
		Where("id = ? AND pool_level = ?", videoID, video.PoolLevelSeed).
		Updates(map[string]interface{}{
			"pool_level":  targetPoolLevel,
			"update_time": w.now(),
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, nil
}

// Run 启动赛马结算调度轮询器，持续周期性执行结算，直到 ctx 被取消。
func (w *TrafficPoolWorker) Run(ctx context.Context) error {
	if w == nil {
		return errors.New("traffic pool worker is nil")
	}

	interval := w.interval
	if interval <= 0 {
		interval = DefaultTrafficPoolPollInterval
	}

	logging.Ctx(ctx).Info("traffic pool worker 启动",
		zap.Duration("interval", interval),
		zap.Int64("batch_limit", w.batchLimit))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 启动时立即尝试执行一次单批次结算
	if _, err := w.SettleBatch(ctx); err != nil && ctx.Err() == nil {
		logging.Ctx(ctx).Warn("traffic pool 初始批次结算异常", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			logging.Ctx(ctx).Info("traffic pool worker 退出")
			return ctx.Err()
		case <-ticker.C:
			if _, err := w.SettleBatch(ctx); err != nil && ctx.Err() == nil {
				logging.Ctx(ctx).Warn("traffic pool 批次结算异常", zap.Error(err))
			}
		}
	}
}

// SettleBatch 触发一次活跃候选池赛马结算批次：
// 1. 基于 Redis ZSet 查询有效曝光 >= 200 的达标候选视频；
// 2. 查询 Redis ZSet 中曝光 < 200 的候选，结合 DB 视频创建时间筛选超过 24 小时兜底窗口的视频；
// 3. 批量执行互动评分计算与 CAS 乐观更新；
// 4. 返回成功完成跃迁的视频数量。
func (w *TrafficPoolWorker) SettleBatch(ctx context.Context) (int, error) {
	if w == nil || w.db == nil {
		return 0, errors.New("traffic pool worker is not initialized")
	}
	if w.cache == nil {
		return 0, nil
	}

	candidatesKey := w.candidatesKey()
	limit := w.batchLimit
	if limit <= 0 {
		limit = DefaultTrafficPoolBatchLimit
	}

	eligibleSet := make(map[uint]struct{})

	// 1. 获取有效曝光达标的候选视频（曝光 >= 200）
	highMembers, err := w.cache.ZRevRangeByScore(ctx, candidatesKey, "+inf", strconv.FormatInt(w.racingExposureThreshold, 10), 0, limit)
	if err != nil {
		return 0, fmt.Errorf("query high exposure candidates from redis: %w", err)
	}
	for _, m := range highMembers {
		if id, err := strconv.ParseUint(m, 10, 64); err == nil && id > 0 {
			eligibleSet[uint(id)] = struct{}{}
		}
	}

	// 2. 到期巡检：获取曝光未达标的候选视频（曝光 < 200），正序遍历优先处理极低曝光长尾，杜绝队头阻塞与长尾饥饿
	lowMembers, err := w.cache.ZRangeByScore(ctx, candidatesKey, "-inf", fmt.Sprintf("(%d", w.racingExposureThreshold), 0, limit)
	if err != nil {
		logging.Ctx(ctx).Warn("query low exposure candidates from redis failed", zap.Error(err))
	} else if len(lowMembers) > 0 {
		var lowIDs []uint
		for _, m := range lowMembers {
			if id, err := strconv.ParseUint(m, 10, 64); err == nil && id > 0 {
				lowIDs = append(lowIDs, uint(id))
			}
		}

		if len(lowIDs) > 0 {
			type videoTime struct {
				ID         uint
				CreateTime time.Time
			}
			var videoTimes []videoTime
			if err := w.db.WithContext(ctx).Model(&video.Video{}).
				Select("id, create_time").
				Where("id IN ?", lowIDs).
				Find(&videoTimes).Error; err == nil {
				now := w.now()
				for _, vt := range videoTimes {
					if now.Sub(vt.CreateTime) >= w.deadlineDuration {
						eligibleSet[vt.ID] = struct{}{}
					}
				}
			}
		}
	}

	// 3. 游标巡检 (Cursor Backfill)：基于主键递增分批向前推进兜底 24h 到期种子视频 (WHERE id > ? ORDER BY id ASC)
	deadlineTime := w.now().Add(-w.deadlineDuration)
	var expiredDBIDs []uint
	query := w.db.WithContext(ctx).Model(&video.Video{}).
		Where("id > ? AND pool_level = ? AND create_time <= ?", w.sweepCursor, video.PoolLevelSeed, deadlineTime).
		Order("id ASC").
		Limit(int(limit))
	if err := query.Pluck("id", &expiredDBIDs).Error; err == nil && len(expiredDBIDs) > 0 {
		w.sweepCursor = expiredDBIDs[len(expiredDBIDs)-1]
		for _, id := range expiredDBIDs {
			eligibleSet[id] = struct{}{}
		}
	} else {
		// 游标推进至末尾或批次为空，重置游标开启下一轮巡检
		w.sweepCursor = 0
	}

	if len(eligibleSet) == 0 {
		return 0, nil
	}

	eligibleIDs := make([]uint, 0, len(eligibleSet))
	for id := range eligibleSet {
		// 过滤短期观察冷却中的视频，防止 5 秒高频空转重复查 DB 算分
		if w.cache != nil {
			evalKey := w.cache.Key("feed:racing:obs_cooldown:%d", id)
			if exists, err := w.cache.Exists(ctx, evalKey); err == nil && exists {
				continue
			}
		}
		eligibleIDs = append(eligibleIDs, id)
	}

	if len(eligibleIDs) == 0 {
		return 0, nil
	}

	results, err := w.SettleCandidates(ctx, eligibleIDs)
	if err != nil {
		return 0, err
	}

	settledCount := 0
	for _, r := range results {
		if r.CasSucceeded {
			settledCount++
		}
	}
	return settledCount, nil
}

// SettleCandidates 对指定的候选视频列表批量执行互动评分计算与 CAS 乐观跃迁。
func (w *TrafficPoolWorker) SettleCandidates(ctx context.Context, videoIDs []uint) ([]SettlementResult, error) {
	if w == nil || w.db == nil {
		return nil, errors.New("traffic pool worker is not initialized")
	}
	if len(videoIDs) == 0 {
		return nil, nil
	}

	// 1. 批量查询视频基本信息
	var videos []video.Video
	if err := w.db.WithContext(ctx).Where("id IN ?", videoIDs).Find(&videos).Error; err != nil {
		return nil, fmt.Errorf("find candidate videos: %w", err)
	}
	if len(videos) == 0 {
		return nil, nil
	}

	// 2. 批量统计评论数
	type countGroup struct {
		VideoID uint  `gorm:"column:video_id"`
		Count   int64 `gorm:"column:count"`
	}
	var commentCounts []countGroup
	_ = w.db.WithContext(ctx).Model(&video.Comment{}).
		Select("video_id, count(*) as count").
		Where("video_id IN ?", videoIDs).
		Group("video_id").
		Find(&commentCounts).Error

	commentMap := make(map[uint]int64, len(commentCounts))
	for _, c := range commentCounts {
		commentMap[c.VideoID] = c.Count
	}

	// 3. 批量统计点赞数（同时兼顾 likes 表行数与 videos.likes_count 列）
	var likeCounts []countGroup
	_ = w.db.WithContext(ctx).Model(&video.Like{}).
		Select("video_id, count(*) as count").
		Where("video_id IN ?", videoIDs).
		Group("video_id").
		Find(&likeCounts).Error

	likeMap := make(map[uint]int64, len(likeCounts))
	for _, l := range likeCounts {
		likeMap[l.VideoID] = l.Count
	}

	candidatesKey := w.candidatesKey()
	now := w.now()
	results := make([]SettlementResult, 0, len(videos))

	for _, v := range videos {
		// 如果该视频已非种子池（已被沉底或晋升），直接清理 Redis 活跃候选队列
		if v.PoolLevel != video.PoolLevelSeed {
			w.removeCandidate(ctx, v.ID)
			continue
		}

		// 获取有效曝光量（优先取 Redis 活跃池 Score 与单视频计数键的最大值）
		var impressions int64
		if w.cache != nil {
			if score, err := w.cache.ZScore(ctx, candidatesKey, strconv.FormatUint(uint64(v.ID), 10)); err == nil {
				impressions = int64(score)
			}
			imprKey := w.cache.Key("feed:video:racing_impressions:%d", v.ID)
			if redisImpr, err := w.cache.GetInt64(ctx, imprKey); err == nil && redisImpr > impressions {
				impressions = redisImpr
			}
		}

		likes := v.LikesCount
		if dbLikes, ok := likeMap[v.ID]; ok && dbLikes > likes {
			likes = dbLikes
		}
		comments := commentMap[v.ID]

		score := CalculateLaplaceScore(likes, comments, impressions)

		// 判定目标 pool_level 与裁决原因
		var targetLevel video.PoolLevel = -1 // -1 表示仍在赛马观察期，暂不跃迁
		reason := ""

		if now.Sub(v.CreateTime) >= w.deadlineDuration {
			// 条件 B: 满 24 小时兜底裁决（生存周期到期）
			if impressions < w.deadlineExposureMin {
				// 曝光不足 50 次，判定为低潜淘汰沉底
				targetLevel = video.PoolLevelSunk
				reason = "deadline_exposure_insufficient"
			} else if score >= w.promotionScoreThreshold {
				// 曝光 >= 50 且平滑分 >= 3% 破格晋升（若有效曝光达标则为赛马达标晋升）
				targetLevel = video.PoolLevelAdvanced
				if impressions >= w.racingExposureThreshold {
					reason = "racing_promoted"
				} else {
					reason = "deadline_promoted"
				}
			} else {
				// 曝光 >= 50 但评分未达 3%，温和沉底
				targetLevel = video.PoolLevelSunk
				reason = "deadline_low_potential"
			}
		} else if impressions >= w.racingExposureThreshold {
			// 条件 A: 未满 24 小时但有效曝光达标 (>= 200) -> 依据 3% 晋升线与 0.8% 沉底线进行三态裁决
			if score >= w.promotionScoreThreshold {
				// Score >= 3% -> 跃迁至进阶池
				targetLevel = video.PoolLevelAdvanced
				reason = "racing_promoted"
			} else if score < w.sinkScoreThreshold {
				// Score < 0.8% -> 极低互动提前沉底淘汰
				targetLevel = video.PoolLevelSunk
				reason = "racing_sunk"
			} else {
				// 0.8% <= Score < 3.0% -> 仍在探索观察期，保留在种子池继续赛马观察
				targetLevel = -1
				reason = "in_observation_window"
			}
		}

		// 仍在观察期，不执行状态跃迁，保留在候选池与种子池中
		if targetLevel == -1 {
			// 设置 1 分钟观察冷却标记，避免 Worker 每 5 秒重复查 DB 算分产生高频空转
			if w.cache != nil {
				evalKey := w.cache.Key("feed:racing:obs_cooldown:%d", v.ID)
				_ = w.cache.SetBytes(ctx, evalKey, []byte("1"), 1*time.Minute)
			}
			results = append(results, SettlementResult{
				VideoID:         v.ID,
				AuthorID:        v.AuthorID,
				Impressions:     impressions,
				Likes:           likes,
				Comments:        comments,
				Score:           score,
				TargetPoolLevel: v.PoolLevel,
				Promoted:        false,
				CasSucceeded:    false,
				Reason:          "in_observation_window",
			})
			continue
		}

		// 执行 MySQL CAS 乐观更新: UPDATE videos SET pool_level = ?, update_time = ? WHERE id = ? AND pool_level = 1
		casSucceeded, err := w.executeCASUpdate(ctx, v.ID, targetLevel)
		if err != nil {
			logging.Ctx(ctx).Error("execute CAS update failed",
				zap.Uint("video_id", v.ID),
				zap.Int("target_level", int(targetLevel)),
				zap.Error(err))
			continue
		}

		promoted := false
		if casSucceeded {
			// 仅当 CAS 跃迁成功且目标层级为进阶池（Level 2）时，调用 SSEHub 广播推送弹窗喜报
			if targetLevel == video.PoolLevelAdvanced {
				promoted = true
				if w.sseHub != nil {
					msg := FormatPromotionMessage(v.Title, v.ID)
					if _, err := w.sseHub.BroadcastToUser(v.AuthorID, EventTypePromotion, v.ID, msg); err != nil {
						logging.Ctx(ctx).Warn("broadcast promotion SSE failed",
							zap.Uint("author_id", v.AuthorID),
							zap.Uint("video_id", v.ID),
							zap.Error(err))
					}
				}
			}
			// 跃迁完成后从 Redis 活跃候选队列移除
			w.removeCandidate(ctx, v.ID)
		} else {
			// CAS 竞争失败（被并发 Worker 抢先处理）：若 DB 中状态已非种子池，移出 Redis 队列
			var curV video.Video
			if err := w.db.WithContext(ctx).Select("pool_level").First(&curV, v.ID).Error; err == nil {
				if curV.PoolLevel != video.PoolLevelSeed {
					w.removeCandidate(ctx, v.ID)
				}
			}
		}

		results = append(results, SettlementResult{
			VideoID:         v.ID,
			AuthorID:        v.AuthorID,
			Impressions:     impressions,
			Likes:           likes,
			Comments:        comments,
			Score:           score,
			TargetPoolLevel: targetLevel,
			Promoted:        promoted,
			CasSucceeded:    casSucceeded,
			Reason:          reason,
		})
	}

	return results, nil
}

// SettleVideo 对单个候选视频执行结算裁决与 CAS 跃迁。
func (w *TrafficPoolWorker) SettleVideo(ctx context.Context, videoID uint) (*SettlementResult, error) {
	if videoID == 0 {
		return nil, errors.New("videoID is required")
	}
	results, err := w.SettleCandidates(ctx, []uint{videoID})
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		return &results[0], nil
	}
	return nil, nil
}
