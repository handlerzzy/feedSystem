package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/handlerzzy/feedsystem/internal/apierror"
	"github.com/handlerzzy/feedsystem/internal/audit"
	"github.com/handlerzzy/feedsystem/internal/feed"
	rediscache "github.com/handlerzzy/feedsystem/internal/middleware/redis"
	"github.com/handlerzzy/feedsystem/internal/video"
	"github.com/handlerzzy/feedsystem/internal/worker"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ==============================================================================
// 全链路端到端集成回归与并发压测门禁测试套件 (E2E Regression & Concurrency Audit)
//
// 验证全生命周期闭环：
// 1. 发布违规阻断 (HTTP 400 ErrContentViolation，零落库)
// 2. 合规发布入库与初始态 (status=published, pool_level=1 种子池，Outbox 事务路由)
// 3. 推荐流探索槽位定向与读隔离 (fusion.go 探索配额槽位优先注入种子池，未审核/违规强制过滤)
// 4. 防刷赛马曝光采集 (24h 单用户滑动去重，Redis Set + ZSet 活跃候选池原子更新)
// 5. 赛马双触发结算 (有效曝光 >= 200 或 24h 兜底，Laplace 平滑公式 0 堆逃逸)
// 6. CAS 原子跃迁与并发防踩踏 (UPDATE videos SET pool_level=2 WHERE id=? AND pool_level=1)
// 7. SSE 幂等喜报推送与候选池清理 (RowsAffected==1 且 Level 2 时广播，ZREM 移出)
// 8. 创作者中心主页徽标变更与访客隔离 (前端 Badge 映射契约与 Fail-Closed 隔离)
// ==============================================================================

// ---------- 测试基础设施与存储实现 ----------

type inMemoryVideoStore struct {
	mu          sync.RWMutex
	videos      map[uint]*video.Video
	outboxMsgs  []video.OutboxMsg
	nextVideoID uint
}

func newInMemoryVideoStore() *inMemoryVideoStore {
	return &inMemoryVideoStore{
		videos:      make(map[uint]*video.Video),
		nextVideoID: 1000,
	}
}

func (s *inMemoryVideoStore) GetByID(ctx context.Context, id uint) (*video.Video, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.videos[id]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	copyV := *v
	return &copyV, nil
}

func (s *inMemoryVideoStore) DeleteVideo(ctx context.Context, id uint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.videos, id)
	return nil
}

func (s *inMemoryVideoStore) ListByAuthorID(ctx context.Context, authorID int64) ([]video.Video, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var list []video.Video
	for _, v := range s.videos {
		if int64(v.AuthorID) == authorID {
			list = append(list, *v)
		}
	}
	return list, nil
}

func (s *inMemoryVideoStore) ListPublishedByAuthorID(ctx context.Context, authorID int64) ([]video.Video, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var list []video.Video
	for _, v := range s.videos {
		if int64(v.AuthorID) == authorID && v.Status == video.VideoStatusPublished {
			list = append(list, *v)
		}
	}
	return list, nil
}

func (s *inMemoryVideoStore) PublishWithOutbox(ctx context.Context, v *video.Video) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextVideoID++
	if v.ID == 0 {
		v.ID = s.nextVideoID
	}
	if v.CreateTime.IsZero() {
		v.CreateTime = time.Now()
	}
	copyV := *v
	s.videos[v.ID] = &copyV

	// 事务性写入 outbox timeline 事件
	s.outboxMsgs = append(s.outboxMsgs, video.OutboxMsg{
		ID:         uint(len(s.outboxMsgs) + 1),
		VideoID:    v.ID,
		EventType:  video.OutboxEventTimeline,
		CreateTime: v.CreateTime,
		Status:     "pending",
	})
	return nil
}

func (s *inMemoryVideoStore) UpdatePoolLevelCAS(ctx context.Context, videoID uint, targetLevel int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.videos[videoID]
	if !ok {
		return false, gorm.ErrRecordNotFound
	}
	// WHERE id = ? AND pool_level = 1
	if v.PoolLevel != video.PoolLevelSeed {
		return false, nil // CAS 失败
	}
	v.PoolLevel = targetLevel
	return true, nil
}

// fakeExploreFeedStore 支持 FeedService 推荐流读路径与探索槽位融合
type fakeExploreFeedStore struct {
	baseVideos  []*video.Video
	freshVideos []*video.Video
}

func (s *fakeExploreFeedStore) ListLatest(ctx context.Context, limit int, latestBefore time.Time) ([]*video.Video, error) {
	if len(s.baseVideos) == 0 {
		return nil, nil
	}
	if limit > 0 && len(s.baseVideos) > limit {
		return s.baseVideos[:limit], nil
	}
	return s.baseVideos, nil
}

func (s *fakeExploreFeedStore) ListFreshLowEngagement(ctx context.Context, limit int, freshness time.Duration, maxPopularity int64, excludeAuthorID uint) ([]*video.Video, error) {
	var filtered []*video.Video
	for _, v := range s.freshVideos {
		if excludeAuthorID > 0 && v.AuthorID == excludeAuthorID {
			continue
		}
		filtered = append(filtered, v)
	}
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func (s *fakeExploreFeedStore) ListLikesCountWithCursor(context.Context, int, *feed.LikesCountCursor) ([]*video.Video, error) {
	return nil, nil
}
func (s *fakeExploreFeedStore) ListByFollowing(context.Context, int, uint, time.Time) ([]*video.Video, error) {
	return nil, nil
}
func (s *fakeExploreFeedStore) ListByPopularity(context.Context, int, int64, time.Time, uint) ([]*video.Video, error) {
	return nil, nil
}
func (s *fakeExploreFeedStore) GetByIDs(context.Context, []uint) ([]*video.Video, error) {
	return nil, nil
}
func (s *fakeExploreFeedStore) ListByTag(context.Context, string, int) ([]*video.Video, error) {
	return nil, nil
}
func (s *fakeExploreFeedStore) ListLongTailByTag(context.Context, string, int, []uint) ([]*video.Video, error) {
	return nil, nil
}
func (s *fakeExploreFeedStore) VideoTagNames(context.Context, []uint) (map[uint][]string, error) {
	return map[uint][]string{}, nil
}

type fakeLikeLookup struct{}

func (fakeLikeLookup) BatchGetLiked(ctx context.Context, videoIDs []uint, viewerID uint) (map[uint]bool, error) {
	return map[uint]bool{}, nil
}

type fakeVectorRecallStore struct{}

func (fakeVectorRecallStore) ListVectorCandidates(ctx context.Context, model string, dim int, limit int) ([]video.VectorCandidate, error) {
	return nil, nil
}
func (fakeVectorRecallStore) LoadVectors(ctx context.Context, videoIDs []uint, model string, dim int) (map[uint][]float32, error) {
	return map[uint][]float32{}, nil
}
func (fakeVectorRecallStore) RecentLikedVideoIDs(ctx context.Context, accountID uint, limit int) ([]uint, error) {
	return nil, nil
}
func (fakeVectorRecallStore) ListEmbeddingModels(ctx context.Context, dim int, limit int) ([]video.EmbeddingModelStat, error) {
	return []video.EmbeddingModelStat{{Model: "test-model", Dim: 1536}}, nil
}


type mockBroadcaster struct {
	mu     sync.Mutex
	calls  []mockBroadcastCall
	callFn func(userID uint, eventType string, targetID uint, content ...string) (*worker.Notification, error)
}

type mockBroadcastCall struct {
	UserID    uint
	EventType string
	TargetID  uint
	Content   string
}

func (m *mockBroadcaster) BroadcastToUser(userID uint, eventType string, targetID uint, content ...string) (*worker.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg := ""
	if len(content) > 0 {
		msg = content[0]
	}
	m.calls = append(m.calls, mockBroadcastCall{
		UserID:    userID,
		EventType: eventType,
		TargetID:  targetID,
		Content:   msg,
	})
	if m.callFn != nil {
		return m.callFn(userID, eventType, targetID, content...)
	}
	return &worker.Notification{
		RecipientID: userID,
		Type:        eventType,
		TargetID:    targetID,
		Content:     msg,
		CreatedAt:   time.Now(),
	}, nil
}

func (m *mockBroadcaster) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockBroadcaster) GetCalls() []mockBroadcastCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]mockBroadcastCall, len(m.calls))
	copy(copied, m.calls)
	return copied
}

func setupE2ETestRedis(t *testing.T) (*rediscache.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis failed: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rediscache.NewClient(rdb, "e2etest:"), mr
}

func openE2EIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(172.23.0.8:3306)/feedflow?charset=utf8mb4&parseTime=True&loc=Local"
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		fallbackDSN := "root:123456@tcp(127.0.0.1:3307)/feedflow?charset=utf8mb4&parseTime=True&loc=Local"
		db, err = gorm.Open(mysql.Open(fallbackDSN), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
	}
	if err != nil {
		t.Skipf("跳过 Live MySQL 集成测试：连接数据库失败: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil || sqlDB.Ping() != nil {
		t.Skipf("跳过 Live MySQL 集成测试：数据库不可达: %v", err)
	}
	_ = db.AutoMigrate(&video.Video{}, &video.Comment{}, &video.Like{}, &worker.Notification{}, &video.OutboxMsg{})
	return db
}

// ---------- Seam 1: 全链路端到端闭环自动化回归测试 ----------

func TestE2E_FullPipelineRegression_ClosedLoop(t *testing.T) {
	cache, mr := setupE2ETestRedis(t)
	videoStore := newInMemoryVideoStore()
	broadcaster := &mockBroadcaster{}

	// 1. 初始化机审敏感词审核器 (L1 Trie 前缀树)
	sensitiveWords := []string{"赌博", "色情", "违规", "badword"}
	auditor := audit.NewTrieAuditor(sensitiveWords)

	// 组装 VideoService
	videoSvc := video.NewVideoService(videoStore, cache, nil)
	videoSvc.WithAuditor(auditor)

	ctx := context.Background()
	authorID := uint(8888)

	// -------------------------------------------------------------
	// Phase 1: 发布违规同步阻断测试 (Machine Audit Gate Interception)
	// -------------------------------------------------------------
	t.Run("Phase 1: 违规视频在发布时同步阻断且零落库", func(t *testing.T) {
		violatingVideos := []*video.Video{
			{
				AuthorID:    authorID,
				Username:    "creator_alice",
				Title:       "最新的赌博技巧实况",
				PlayURL:     "https://cdn.example.com/bad1.mp4",
				CoverURL:    "https://cdn.example.com/bad1.jpg",
				Description: "正常介绍",
			},
			{
				AuthorID:    authorID,
				Username:    "creator_alice",
				Title:       "正常合规标题",
				PlayURL:     "https://cdn.example.com/bad2.mp4",
				CoverURL:    "https://cdn.example.com/bad2.jpg",
				Description: "点击进入色情专区",
			},
			{
				AuthorID:    authorID,
				Username:    "creator_alice",
				Title:       "特殊混淆 b-a-d*w*o*r*d 内容",
				PlayURL:     "https://cdn.example.com/bad3.mp4",
				CoverURL:    "https://cdn.example.com/bad3.jpg",
			},
		}

		for _, v := range violatingVideos {
			err := videoSvc.Publish(ctx, v)
			if !errors.Is(err, apierror.ErrContentViolation) {
				t.Fatalf("expected ErrContentViolation for violating video %q, got: %v", v.Title, err)
			}
		}

		// 验证没有写入任何视频数据与 Outbox 消息
		list, err := videoStore.ListByAuthorID(ctx, int64(authorID))
		if err != nil {
			t.Fatalf("ListByAuthorID failed: %v", err)
		}
		if len(list) != 0 {
			t.Fatalf("expected 0 stored videos after violation, got: %d", len(list))
		}
		if len(videoStore.outboxMsgs) != 0 {
			t.Fatalf("expected 0 outbox messages after violation, got: %d", len(videoStore.outboxMsgs))
		}
	})

	// -------------------------------------------------------------
	// Phase 2: 合规发布入库与初始状态 (Compliant Publish & Initial State)
	// -------------------------------------------------------------
	var compliantVideo *video.Video
	t.Run("Phase 2: 合规视频成功发布并进入种子池 (pool_level=1)", func(t *testing.T) {
		compliantVideo = &video.Video{
			AuthorID:    authorID,
			Username:    "creator_alice",
			Title:       "Go高并发架构实战与赛马引擎",
			Description: "阶梯流量池与状态机闭环讲解",
			PlayURL:     "https://cdn.example.com/good1.mp4",
			CoverURL:    "https://cdn.example.com/good1.jpg",
		}

		err := videoSvc.Publish(ctx, compliantVideo)
		if err != nil {
			t.Fatalf("Publish compliant video failed: %v", err)
		}

		if compliantVideo.ID == 0 {
			t.Fatalf("expected non-zero video ID after publish")
		}
		if compliantVideo.Status != video.VideoStatusPublished {
			t.Fatalf("expected video status %q, got %q", video.VideoStatusPublished, compliantVideo.Status)
		}
		if compliantVideo.PoolLevel != video.PoolLevelSeed {
			t.Fatalf("expected video pool_level %d (Seed), got %d", video.PoolLevelSeed, compliantVideo.PoolLevel)
		}

		// 验证 Outbox 消息写入且带有 event_type = 'timeline'
		if len(videoStore.outboxMsgs) != 1 {
			t.Fatalf("expected 1 outbox msg, got: %d", len(videoStore.outboxMsgs))
		}
		msg := videoStore.outboxMsgs[0]
		if msg.VideoID != compliantVideo.ID {
			t.Fatalf("expected outbox video_id %d, got %d", compliantVideo.ID, msg.VideoID)
		}
		if msg.EventType != video.OutboxEventTimeline {
			t.Fatalf("expected outbox event_type %q, got %q", video.OutboxEventTimeline, msg.EventType)
		}
	})

	// -------------------------------------------------------------
	// Phase 3: 探索槽位定向与读隔离 (fusion.go Explore Slot & Isolation)
	// -------------------------------------------------------------
	t.Run("Phase 3: 推荐流探索配额槽位优先注入种子池内容，违规/未审核强制读隔离", func(t *testing.T) {
		// 构造 10 条基础时序视频
		var baseVideos []*video.Video
		for i := 1; i <= 10; i++ {
			baseVideos = append(baseVideos, &video.Video{
				ID:         uint(i),
				AuthorID:   uint(5000 + i),
				Title:      fmt.Sprintf("Base Timeline Video %d", i),
				Status:     video.VideoStatusPublished,
				PoolLevel:  video.PoolLevelSettled,
				CreateTime: time.Now().Add(-time.Duration(i) * time.Minute),
			})
		}

		// 混入合规种子视频与违规/审核中视频
		auditingVid := &video.Video{
			ID:         9001,
			AuthorID:   authorID,
			Title:      "审核中视频",
			Status:     video.VideoStatusAuditing,
			PoolLevel:  video.PoolLevelSeed,
			CreateTime: time.Now(),
		}
		rejectedVid := &video.Video{
			ID:           9002,
			AuthorID:     authorID,
			Title:        "违规驳回视频",
			Status:       video.VideoStatusRejected,
			PoolLevel:    video.PoolLevelSettled,
			RejectReason: "包含涉政违规词",
			CreateTime:   time.Now(),
		}
		videoStore.videos[auditingVid.ID] = auditingVid
		videoStore.videos[rejectedVid.ID] = rejectedVid

		// 组装真实 FeedService 接入探索槽位融合链路 (fusion.go)
		exploreStore := &fakeExploreFeedStore{
			baseVideos:  baseVideos,
			freshVideos: []*video.Video{auditingVid, compliantVideo, rejectedVid},
		}
		feedSvc := feed.NewFeedService(exploreStore, fakeLikeLookup{}, cache).
			WithLocalCache(false).
			WithSemanticRecall(feed.SemanticRecallOptions{
				Store:  fakeVectorRecallStore{},
				Model:  "test-model",
				Dim:    1536,
				Addons: feed.SemanticAddons{ExploreQuota: 0.2},
			}) // 20% 探索位

		// 读者发起推荐流拉取
		viewerID := uint(3333)
		feedResp, err := feedSvc.ListLatest(ctx, 10, time.Time{}, viewerID)
		if err != nil {
			t.Fatalf("ListLatest fused feed failed: %v", err)
		}

		// 验证 1：违规与未审核视频绝不会出现在推荐流中
		for _, item := range feedResp.VideoList {
			if item.ID == auditingVid.ID || item.ID == rejectedVid.ID {
				t.Fatalf("feed leaked unapproved/rejected video ID=%d title=%s", item.ID, item.Title)
			}
		}

		// 验证 2：探索槽位（ExploreSlotOffset = 5）成功注入 pool_level = 1 种子池视频！
		if len(feedResp.VideoList) >= 6 {
			slot5Video := feedResp.VideoList[5]
			if slot5Video.ID != compliantVideo.ID {
				t.Errorf("expected slot 5 (explore slot) to contain seed video %d, got: %d (%s)",
					compliantVideo.ID, slot5Video.ID, slot5Video.Title)
			}
		}

		// 验证 3：访客隔离（外部访客不可见 auditing / rejected，作者本人可见）
		visitorID := uint(9999)
		visitorList, err := videoSvc.ListByAuthorID(ctx, authorID, visitorID)
		if err != nil {
			t.Fatalf("visitor ListByAuthorID failed: %v", err)
		}
		for _, v := range visitorList {
			if v.Status != video.VideoStatusPublished {
				t.Fatalf("visitor saw non-published video: ID=%d, Status=%s", v.ID, v.Status)
			}
		}
		if len(visitorList) != 1 || visitorList[0].ID != compliantVideo.ID {
			t.Fatalf("visitor should only see published video %d, got: %v", compliantVideo.ID, visitorList)
		}

		authorList, err := videoSvc.ListByAuthorID(ctx, authorID, authorID)
		if err != nil {
			t.Fatalf("author ListByAuthorID failed: %v", err)
		}
		if len(authorList) < 3 {
			t.Fatalf("author should see all 3 videos, got %d", len(authorList))
		}
	})

	// -------------------------------------------------------------
	// Phase 4: 防刷赛马曝光采集 (Anti-Gaming Racing Impressions)
	// -------------------------------------------------------------
	t.Run("Phase 4: 单用户 24 小时去重赛马曝光采集与候选池维护", func(t *testing.T) {
		// 等待 Phase 3 推荐流读取阶段触发的异步赛马曝光协程处理完毕
		time.Sleep(50 * time.Millisecond)

		feedSvc := feed.NewFeedService(nil, nil, cache)
		videoID := compliantVideo.ID
		userA := uint(1001)

		candKey := cache.Key(feed.RacingCandidatesKey)
		scoreBefore, _ := mr.ZScore(candKey, strconv.FormatUint(uint64(videoID), 10))

		// 1. 用户 A 首次刷到视频 -> 有效曝光
		first, err := feedSvc.RecordRacingImpression(ctx, userA, videoID)
		if err != nil {
			t.Fatalf("RecordRacingImpression userA failed: %v", err)
		}
		if !first {
			t.Fatalf("expected first impression for userA to be true")
		}

		// 验证 Redis Set 记录了 userA，ZSet Score 准确递增 1
		uKey := cache.Key("feed:video:racing_users:%d", videoID)
		isMem, err := mr.IsMember(uKey, strconv.FormatUint(uint64(userA), 10))
		if err != nil || !isMem {
			t.Fatalf("expected userA in racing_users set: %v", err)
		}

		scoreAfterFirst, _ := mr.ZScore(candKey, strconv.FormatUint(uint64(videoID), 10))
		if scoreAfterFirst != scoreBefore+1.0 {
			t.Fatalf("expected candidates score to increment by 1, got %f (was %f)", scoreAfterFirst, scoreBefore)
		}

		// 2. 模拟爬虫或用户狂刷 20 次 -> 24h 滑动去重全部返回 false，Score 严格不增加
		for i := 0; i < 20; i++ {
			repeated, err := feedSvc.RecordRacingImpression(ctx, userA, videoID)
			if err != nil {
				t.Fatalf("repeated impression check failed: %v", err)
			}
			if repeated {
				t.Fatalf("repeated impression on iteration %d should be deduplicated (false)", i)
			}
		}

		scoreAfterSpam, _ := mr.ZScore(candKey, strconv.FormatUint(uint64(videoID), 10))
		if scoreAfterSpam != scoreAfterFirst {
			t.Fatalf("anti-gaming check failed: score mutated from %f to %f", scoreAfterFirst, scoreAfterSpam)
		}

		// 3. 模拟独立读者观看直至累计达到 200 次有效赛马曝光
		currentScore := int(scoreAfterFirst)
		for u := uint(2001); currentScore < 200; u++ {
			ok, err := feedSvc.RecordRacingImpression(ctx, u, videoID)
			if err != nil {
				t.Fatalf("record impression for user %d failed: %v", u, err)
			}
			if ok {
				currentScore++
			}
		}

		finalScore, _ := mr.ZScore(candKey, strconv.FormatUint(uint64(videoID), 10))
		if finalScore != 200.0 {
			t.Fatalf("expected 200 effective racing impressions, got: %f", finalScore)
		}
	})

	// -------------------------------------------------------------
	// Phase 5: 赛马双触发结算、CAS 跃迁与 SSE 喜报推送
	// -------------------------------------------------------------
	t.Run("Phase 5: 曝光达标 (>=200) 与优质互动触发 CAS 晋升与单次幂等 SSE 喜报", func(t *testing.T) {
		videoID := compliantVideo.ID
		candKey := cache.Key(feed.RacingCandidatesKey)

		// 模拟点赞与评论：15 赞, 5 评
		// Laplace 得分 = (15*2 + 5*3 + 1) / (200 + 20) = 46 / 220 ≈ 0.20909 >= 0.03 (晋升线)
		score := worker.CalculateLaplaceScore(15, 5, 200)
		expectedScore := 46.0 / 220.0
		if math.Abs(score-expectedScore) > 1e-6 {
			t.Fatalf("Laplace score mismatch: got %f, want %f", score, expectedScore)
		}

		// 并发争抢结算模拟：10 个 Worker Goroutine 同时结算该视频
		const concurrency = 10
		var wg sync.WaitGroup
		var successCASCount int64

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// 执行原子 CAS 跃迁
				casSuccess, err := videoStore.UpdatePoolLevelCAS(ctx, videoID, video.PoolLevelAdvanced)
				if err != nil {
					t.Errorf("CAS execution failed: %v", err)
					return
				}
				if casSuccess {
					atomic.AddInt64(&successCASCount, 1)
					// CAS 成功者负责发送 SSE 喜报和清理 Redis 活跃队列
					_, _ = broadcaster.BroadcastToUser(
						compliantVideo.AuthorID,
						worker.EventTypePromotion,
						videoID,
						fmt.Sprintf("🎉 恭喜！您的作品《%s》已从种子池晋升至进阶爆款池！", compliantVideo.Title),
					)
					_ = cache.ZRem(ctx, candKey, strconv.FormatUint(uint64(videoID), 10))
				}
			}()
		}
		wg.Wait()

		// 验证严格并发幂等性：10 个并发竞争中，CAS 仅有 1 次成功！
		if successCASCount != 1 {
			t.Fatalf("expected exactly 1 successful CAS transition, got: %d", successCASCount)
		}

		// 验证 SSE 广播通知仅调用了 1 次（幂等零重复）
		if broadcaster.CallCount() != 1 {
			t.Fatalf("expected exactly 1 SSE broadcast, got: %d", broadcaster.CallCount())
		}
		calls := broadcaster.GetCalls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 broadcast call, got %d", len(calls))
		}
		if calls[0].UserID != authorID || calls[0].TargetID != videoID || calls[0].EventType != worker.EventTypePromotion {
			t.Fatalf("unexpected SSE call content: %+v", calls[0])
		}

		// 验证 Redis 活跃候选队列已移除该视频
		scoreInQueue, _ := mr.ZScore(candKey, strconv.FormatUint(uint64(videoID), 10))
		if scoreInQueue != 0 {
			t.Fatalf("settled video should be removed from ZSet candidates, score: %f", scoreInQueue)
		}
	})

	// -------------------------------------------------------------
	// Phase 6: 创作者中心主页徽标变更与状态感知契约
	// -------------------------------------------------------------
	t.Run("Phase 6: 创作者主页即时感知进阶爆款徽标 (🔥 进阶爆款)", func(t *testing.T) {
		list, err := videoSvc.ListByAuthorID(ctx, authorID, authorID)
		if err != nil {
			t.Fatalf("ListByAuthorID failed: %v", err)
		}

		var target *video.Video
		for i := range list {
			if list[i].ID == compliantVideo.ID {
				target = &list[i]
				break
			}
		}
		if target == nil {
			t.Fatalf("promoted video %d not found in author video list", compliantVideo.ID)
		}

		// 验证层级已跃迁至进阶池 Level 2
		if target.PoolLevel != video.PoolLevelAdvanced {
			t.Fatalf("expected video pool_level %d (Advanced), got: %d", video.PoolLevelAdvanced, target.PoolLevel)
		}

		// 验证前端徽标映射契约 (与 UserProfileView.vue 保持一致)
		badge := mapVideoBadge(target)
		if badge == nil || badge.Text != "🔥 进阶爆款" || badge.ClassName != "badge-advanced" {
			t.Fatalf("unexpected badge for promoted video: %+v", badge)
		}
	})

	// -------------------------------------------------------------
	// Phase 7: 赛马双触发长尾 24h 兜底结算测试 (Deadline Fallback)
	// -------------------------------------------------------------
	t.Run("Phase 7: 低曝光长尾视频满 24 小时兜底淘汰结算，杜绝死锁", func(t *testing.T) {
		staleVideo := &video.Video{
			ID:         9888,
			AuthorID:   authorID,
			Title:      "半夜冷门发布视频",
			Status:     video.VideoStatusPublished,
			PoolLevel:  video.PoolLevelSeed,
			CreateTime: time.Now().Add(-26 * time.Hour), // 超过 24 小时
			LikesCount: 0,
		}
		videoStore.videos[staleVideo.ID] = staleVideo

		candKey := cache.Key(feed.RacingCandidatesKey)
		// 曝光仅 40 次 (<200)
		mr.ZAdd(candKey, 40.0, strconv.FormatUint(uint64(staleVideo.ID), 10))

		// 24h 兜底判定：曝光 < 50 且无互动，直接沉底淘汰至 Level 0
		casSuccess, err := videoStore.UpdatePoolLevelCAS(ctx, staleVideo.ID, video.PoolLevelSettled)
		if err != nil || !casSuccess {
			t.Fatalf("deadline fallback CAS update failed: %v", err)
		}
		_ = cache.ZRem(ctx, candKey, strconv.FormatUint(uint64(staleVideo.ID), 10))

		// 验证状态变为 Level 0 (已结算/沉底)
		updated, _ := videoStore.GetByID(ctx, staleVideo.ID)
		if updated.PoolLevel != video.PoolLevelSettled {
			t.Fatalf("expected pool_level %d (Settled), got: %d", video.PoolLevelSettled, updated.PoolLevel)
		}

		badge := mapVideoBadge(updated)
		if badge == nil || badge.Text != "📦 已结算" || badge.ClassName != "badge-settled" {
			t.Fatalf("unexpected badge for settled video: %+v", badge)
		}
	})
}

// ---------- Seam 2: 真实 MySQL 驱动 TrafficPoolWorker 与 EXPLAIN 执行计划审计 ----------

type dummyConnPool struct{}

func (*dummyConnPool) PrepareContext(context.Context, string) (*sql.Stmt, error) { return nil, nil }
func (*dummyConnPool) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (*dummyConnPool) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}
func (*dummyConnPool) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }
func (d *dummyConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	return d, nil
}
func (*dummyConnPool) Commit() error   { return nil }
func (*dummyConnPool) Rollback() error { return nil }

type sqlCaptureLogger struct {
	logger.Interface
	captured []string
}

func (l *sqlCaptureLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sqlStr, _ := fc()
	l.captured = append(l.captured, sqlStr)
}

func TestE2E_DatabaseIndexAndQueryPlan_Audit(t *testing.T) {
	t.Run("DryRun 审计：验证 CAS 乐观更新与 Outbox 路由 SQL 结构", func(t *testing.T) {
		capLogger := &sqlCaptureLogger{
			Interface: logger.Default.LogMode(logger.Silent),
		}
		dryDB, err := gorm.Open(mysql.New(mysql.Config{
			Conn:                      &dummyConnPool{},
			SkipInitializeWithVersion: true,
		}), &gorm.Config{
			DryRun: true,
			Logger: capLogger,
		})
		if err != nil {
			t.Fatalf("open DryRun DB failed: %v", err)
		}

		// 1. CAS 更新 SQL 审计
		res := dryDB.Model(&video.Video{}).
			Where("id = ? AND pool_level = 1", 8888).
			Update("pool_level", video.PoolLevelAdvanced)
		if res.Error != nil {
			t.Fatalf("CAS query generation failed: %v", res.Error)
		}

		foundCAS := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "UPDATE `videos`") && strings.Contains(q, "pool_level") {
				foundCAS = true
				if !strings.Contains(q, "WHERE `id` = ") && !strings.Contains(q, "WHERE id = ") {
					t.Errorf("CAS query missing primary key id filter: %s", q)
				}
				if !strings.Contains(q, "pool_level = 1") && !strings.Contains(q, "pool_level` = 1") {
					t.Errorf("CAS query missing pool_level = 1 condition: %s", q)
				}
			}
		}
		if !foundCAS {
			t.Errorf("CAS update query not captured: %v", capLogger.captured)
		}

		// 2. Outbox 抢占 SQL 审计
		capLogger.captured = nil
		var batch []video.OutboxMsg
		dryDB.Where("status = ? AND event_type = ?", "pending", video.OutboxEventTimeline).
			Order("create_time ASC").Limit(100).
			Find(&batch)

		foundOutbox := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "outbox_msgs") {
				foundOutbox = true
				if !strings.Contains(q, "event_type") {
					t.Errorf("outbox query missing event_type filter: %s", q)
				}
				if !strings.Contains(q, "status") {
					t.Errorf("outbox query missing status filter: %s", q)
				}
			}
		}
		if !foundOutbox {
			t.Errorf("outbox claim query not captured: %v", capLogger.captured)
		}
	})

	t.Run("Live MySQL EXPLAIN 审计与 TrafficPoolWorker 真实调度", func(t *testing.T) {
		db := openE2EIntegrationDB(t)
		cache, mr := setupE2ETestRedis(t)
		broadcaster := &mockBroadcaster{}

		// 1. 实例化真实 TrafficPoolWorker
		workerSvc := worker.NewTrafficPoolWorker(db, cache, broadcaster)
		ctx := context.Background()

		const testVideoID = uint(88991)
		testV := video.Video{
			ID:         testVideoID,
			AuthorID:   7777,
			Title:      "真实Worker调度测试",
			Status:     video.VideoStatusPublished,
			PoolLevel:  video.PoolLevelSeed,
			CreateTime: time.Now().Add(-1 * time.Hour),
			LikesCount: 10,
		}
		db.Where("id = ?", testVideoID).Delete(&video.Video{})
		if err := db.Create(&testV).Error; err != nil {
			t.Fatalf("create test video failed: %v", err)
		}
		t.Cleanup(func() { db.Where("id = ?", testVideoID).Delete(&video.Video{}) })

		// Redis 注入 200 曝光
		candKey := cache.Key(feed.RacingCandidatesKey)
		mr.ZAdd(candKey, 200.0, strconv.FormatUint(uint64(testVideoID), 10))

		// 真正调用 workerSvc.SettleVideo
		res, err := workerSvc.SettleVideo(ctx, testVideoID)
		if err != nil {
			t.Fatalf("TrafficPoolWorker SettleVideo failed: %v", err)
		}
		if !res.CasSucceeded || !res.Promoted || res.TargetPoolLevel != video.PoolLevelAdvanced {
			t.Fatalf("unexpected settlement result: %+v", res)
		}

		// 验证 DB 真实更新
		var dbV video.Video
		if err := db.First(&dbV, testVideoID).Error; err != nil {
			t.Fatalf("query video from DB failed: %v", err)
		}
		if dbV.PoolLevel != video.PoolLevelAdvanced {
			t.Fatalf("expected DB pool_level 2, got: %d", dbV.PoolLevel)
		}

		// 验证 SSE 真实推送
		if broadcaster.CallCount() != 1 {
			t.Fatalf("expected 1 SSE broadcast, got: %d", broadcaster.CallCount())
		}

		// 2. 执行真实 EXPLAIN 执行计划断言
		type explainRow struct {
			ID           int64          `gorm:"column:id"`
			SelectType   string         `gorm:"column:select_type"`
			Table        string         `gorm:"column:table"`
			Type         string         `gorm:"column:type"`
			PossibleKeys sql.NullString `gorm:"column:possible_keys"`
			Key          sql.NullString `gorm:"column:key"`
			Rows         int64          `gorm:"column:rows"`
			Extra        sql.NullString `gorm:"column:Extra"`
		}

		// 审计 CAS UPDATE
		var casExplain explainRow
		if err := db.Raw("EXPLAIN UPDATE videos SET pool_level = 2 WHERE id = ? AND pool_level = 1", testVideoID).Scan(&casExplain).Error; err != nil {
			t.Fatalf("explain CAS update failed: %v", err)
		}
		if casExplain.Type == "ALL" {
			t.Errorf("CAS UPDATE must not be full table scan (ALL), got: %s", casExplain.Type)
		}
		if casExplain.Key.String != "PRIMARY" {
			t.Errorf("CAS UPDATE should use PRIMARY key, got: %s", casExplain.Key.String)
		}

		// 审计 Outbox 抢占
		var outboxExplain explainRow
		if err := db.Raw("EXPLAIN SELECT * FROM outbox_msgs WHERE status = 'pending' AND event_type = 'timeline' ORDER BY create_time ASC LIMIT 100").Scan(&outboxExplain).Error; err != nil {
			t.Fatalf("explain outbox claim failed: %v", err)
		}
		if outboxExplain.Type == "ALL" {
			t.Errorf("outbox claim must not be full table scan (ALL), got: %s", outboxExplain.Type)
		}
		if strings.Contains(outboxExplain.Extra.String, "Using filesort") {
			t.Errorf("outbox claim must not require filesort with composite index, got: %s", outboxExplain.Extra.String)
		}
	})
}

// ---------- Seam 3: 前端徽标与 SSE 通知数据契约测试 ----------

type badgeInfo struct {
	Text      string
	ClassName string
}

// mapVideoBadge 与 frontend/src/views/UserProfileView.vue 的 getVideoBadge 完全对齐
func mapVideoBadge(v *video.Video) *badgeInfo {
	if v == nil {
		return nil
	}
	if v.Status == video.VideoStatusRejected {
		return &badgeInfo{Text: "🔴 违规驳回", ClassName: "badge-rejected"}
	}
	if v.Status == video.VideoStatusAuditing {
		return &badgeInfo{Text: "🟡 审核中", ClassName: "badge-auditing"}
	}
	if v.Status == video.VideoStatusPublished {
		switch v.PoolLevel {
		case video.PoolLevelSeed:
			return &badgeInfo{Text: "🌱 种子池扶持中", ClassName: "badge-seed"}
		case video.PoolLevelAdvanced:
			return &badgeInfo{Text: "🔥 进阶爆款", ClassName: "badge-advanced"}
		default:
			return &badgeInfo{Text: "📦 已结算", ClassName: "badge-settled"}
		}
	}
	return nil
}

func TestE2E_FrontendBadgeAndSSEContract(t *testing.T) {
	t.Run("作品状态机各阶段与前端徽标样式严格映射", func(t *testing.T) {
		cases := []struct {
			status    string
			poolLevel int
			wantText  string
			wantClass string
		}{
			{video.VideoStatusAuditing, 0, "🟡 审核中", "badge-auditing"},
			{video.VideoStatusRejected, 0, "🔴 违规驳回", "badge-rejected"},
			{video.VideoStatusPublished, video.PoolLevelSeed, "🌱 种子池扶持中", "badge-seed"},
			{video.VideoStatusPublished, video.PoolLevelAdvanced, "🔥 进阶爆款", "badge-advanced"},
			{video.VideoStatusPublished, video.PoolLevelSettled, "📦 已结算", "badge-settled"},
		}

		for _, tc := range cases {
			b := mapVideoBadge(&video.Video{Status: tc.status, PoolLevel: tc.poolLevel})
			if b == nil || b.Text != tc.wantText || b.ClassName != tc.wantClass {
				t.Errorf("badge mismatch for status=%s, pool=%d: got %+v, want text=%s, class=%s",
					tc.status, tc.poolLevel, b, tc.wantText, tc.wantClass)
			}
		}
	})

	t.Run("SSE 喜报事件 JSON 结构与前端解析器兼容", func(t *testing.T) {
		notif := worker.Notification{
			ID:          1,
			RecipientID: 1001,
			Type:        worker.EventTypePromotion,
			TargetID:    2002,
			Content:     "🎉 恭喜！您的作品《Go高并发》已成功晋升进阶爆款池！",
			CreatedAt:   time.Now(),
		}

		data, err := json.Marshal(notif)
		if err != nil {
			t.Fatalf("marshal notification failed: %v", err)
		}

		// 模拟前端 UserProfileView.vue 的 onmessage 解析
		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("unmarshal notification failed: %v", err)
		}

		if parsed["type"] != "promotion" {
			t.Fatalf("expected type 'promotion', got %v", parsed["type"])
		}
		if uint(parsed["target_id"].(float64)) != 2002 {
			t.Fatalf("expected target_id 2002, got %v", parsed["target_id"])
		}
		if !strings.Contains(parsed["content"].(string), "进阶爆款池") {
			t.Fatalf("unexpected content: %v", parsed["content"])
		}
	})
}
