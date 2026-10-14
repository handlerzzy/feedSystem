package worker

import (
	"context"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rediscache "github.com/handlerzzy/feedsystem/internal/middleware/redis"
	"github.com/handlerzzy/feedsystem/internal/video"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ---------- Seam 1: Laplace 平滑互动评分计算 ----------

func TestCalculateLaplaceScore(t *testing.T) {
	tests := []struct {
		name        string
		likes       int64
		comments    int64
		impressions int64
		expected    float64
	}{
		{
			name:        "零互动曝光200次 (极低互动沉底)",
			likes:       0,
			comments:    0,
			impressions: 200,
			// (0*2 + 0*3 + 1) / (200 + 20) = 1 / 220 ≈ 0.00454545
			expected: 1.0 / 220.0,
		},
		{
			name:        "3赞0评曝光200次 (达标晋升线 3% 以上)",
			likes:       3,
			comments:    0,
			impressions: 200,
			// (3*2 + 0*3 + 1) / 220 = 7 / 220 ≈ 0.03181818
			expected: 7.0 / 220.0,
		},
		{
			name:        "0赞2评曝光200次 (达标晋升线 3% 以上)",
			likes:       0,
			comments:    2,
			impressions: 200,
			// (0*2 + 2*3 + 1) / 220 = 7 / 220 ≈ 0.03181818
			expected: 7.0 / 220.0,
		},
		{
			name:        "1赞0评曝光200次 (介于 0.8% 与 3% 之间观察区间)",
			likes:       1,
			comments:    0,
			impressions: 200,
			// (1*2 + 0 + 1) / 220 = 3 / 220 ≈ 0.01363636
			expected: 3.0 / 220.0,
		},
		{
			name:        "小样本先验平滑约束 (防极端小样本扰动)",
			likes:       1,
			comments:    0,
			impressions: 1,
			// 朴素互动率为 100%，Laplace 平滑后为 (2*1 + 1) / (1 + 20) = 3 / 21 ≈ 0.142857
			expected: 3.0 / 21.0,
		},
		{
			name:        "负数防御边界",
			likes:       -5,
			comments:    -2,
			impressions: -10,
			// 负数置为0：(0 + 0 + 1) / (0 + 20) = 1 / 20 = 0.05
			expected: 1.0 / 20.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := CalculateLaplaceScore(tt.likes, tt.comments, tt.impressions)
			if math.Abs(score-tt.expected) > 1e-6 {
				t.Errorf("CalculateLaplaceScore(%d, %d, %d) = %f, want %f",
					tt.likes, tt.comments, tt.impressions, score, tt.expected)
			}
		})
	}
}

// ---------- Seam 2: SSEHub BroadcastToUser 广播与喜报推送 ----------

func TestSSEHub_BroadcastToUser(t *testing.T) {
	hub := NewSSEHub(nil)
	const userID = uint(10086)
	const videoID = uint(2001)
	const content = "恭喜！您的作品《Go高并发实战》已成功晋升进阶爆款池！"

	ch := hub.Subscribe(userID)
	defer hub.Unsubscribe(userID, ch)

	notif, err := hub.BroadcastToUser(userID, EventTypePromotion, videoID, content)
	if err != nil {
		t.Fatalf("BroadcastToUser failed: %v", err)
	}

	if notif.RecipientID != userID {
		t.Errorf("RecipientID = %d, want %d", notif.RecipientID, userID)
	}
	if notif.Type != EventTypePromotion {
		t.Errorf("Type = %s, want %s", notif.Type, EventTypePromotion)
	}
	if notif.TargetID != videoID {
		t.Errorf("TargetID = %d, want %d", notif.TargetID, videoID)
	}
	if notif.Content != content {
		t.Errorf("Content = %s, want %s", notif.Content, content)
	}

	// 验证订阅者通道收到该喜报
	select {
	case received := <-ch:
		if received.ID != notif.ID || received.Content != content {
			t.Errorf("received notif mismatch: %+v", received)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber channel timed out waiting for broadcast")
	}
}

func TestSSEHub_BroadcastToUser_WithDBAndDefaultContent(t *testing.T) {
	db := openWorkerIntegrationDB(t)
	hub := NewSSEHub(db)
	const userID = uint(10087)
	const videoID = uint(2002)

	// 不传 content，测试默认文案与 DB 落库
	notif, err := hub.BroadcastToUser(userID, EventTypePromotion, videoID)
	if err != nil {
		t.Fatalf("BroadcastToUser failed: %v", err)
	}

	if notif.ID == 0 {
		t.Errorf("expected notif.ID to be assigned by DB AutoIncrement")
	}
	if notif.Content != "恭喜！您的作品已成功晋升进阶推荐池！" {
		t.Errorf("unexpected default content: %s", notif.Content)
	}

	// 从 DB 验证落库记录
	var dbNotif Notification
	if err := db.First(&dbNotif, notif.ID).Error; err != nil {
		t.Fatalf("failed to query notification from DB: %v", err)
	}
	t.Cleanup(func() { db.Delete(&Notification{}, notif.ID) })

	if dbNotif.RecipientID != userID || dbNotif.Type != EventTypePromotion || dbNotif.TargetID != videoID {
		t.Errorf("DB notification mismatch: %+v", dbNotif)
	}
}


// ---------- Seam 3: CAS 条件更新 SQL 生成与 DryRun 审计 ----------

func TestTrafficPoolWorker_CAS_SQLGeneration(t *testing.T) {
	capLogger := &sqlCaptureLogger{}
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

	worker := NewTrafficPoolWorker(dryDB, nil, nil)

	// 调用内部 CAS 执行方法
	_, _ = worker.executeCASUpdate(context.Background(), 12345, video.PoolLevelAdvanced)

	foundCASUpdate := false
	for _, q := range capLogger.captured {
		if strings.Contains(q, "UPDATE `videos`") && strings.Contains(q, "pool_level") && strings.Contains(q, "WHERE") {
			foundCASUpdate = true
			if !strings.Contains(q, "pool_level` = 1") && !strings.Contains(q, "pool_level = 1") && !strings.Contains(q, "pool_level` = ?") {
				t.Errorf("CAS update missing pool_level = 1 condition: %s", q)
			}
			if !strings.Contains(q, "update_time") {
				t.Errorf("CAS update missing update_time column: %s", q)
			}
		}
	}

	if !foundCASUpdate {
		t.Errorf("expected CAS update query on videos table, got queries: %v", capLogger.captured)
	}
}

// ---------- 辅助测试工具 ----------

type mockBroadcaster struct {
	mu     sync.Mutex
	calls  []mockBroadcastCall
	callFn func(userID uint, eventType string, targetID uint, content ...string) (*Notification, error)
}

type mockBroadcastCall struct {
	UserID    uint
	EventType string
	TargetID  uint
	Content   string
}

func (m *mockBroadcaster) BroadcastToUser(userID uint, eventType string, targetID uint, content ...string) (*Notification, error) {
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
	return &Notification{
		RecipientID: userID,
		Type:        eventType,
		TargetID:    targetID,
		Content:     msg,
		CreatedAt:   time.Now(),
	}, nil
}

func (m *mockBroadcaster) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func setupTestRedis(t *testing.T) (*rediscache.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis failed: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rediscache.NewClient(rdb, "test:"), mr
}

func openWorkerIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(172.23.0.8:3306)/feedflow?charset=utf8mb4&parseTime=True&loc=Local"
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		// 备用端口重试
		fallbackDSN := "root:123456@tcp(127.0.0.1:3307)/feedflow?charset=utf8mb4&parseTime=True&loc=Local"
		db, err = gorm.Open(mysql.Open(fallbackDSN), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
	}
	if err != nil {
		t.Skipf("跳过 MySQL 集成测试：连接数据库失败: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil || sqlDB.Ping() != nil {
		t.Skipf("跳过 MySQL 集成测试：数据库不可达: %v", err)
	}
	_ = db.AutoMigrate(&video.Video{}, &video.Comment{}, &video.Like{}, &Notification{})
	return db
}

// ---------- Seam 4: 赛马双触发结算、CAS 跃迁与 Redis 候选移除集成测试 ----------

func TestTrafficPoolWorker_SettleCandidates_Thresholds(t *testing.T) {
	db := openWorkerIntegrationDB(t)
	cache, mr := setupTestRedis(t)
	broadcaster := &mockBroadcaster{}

	worker := NewTrafficPoolWorker(db, cache, broadcaster)

	ctx := context.Background()
	candidatesKey := cache.Key("feed:racing:candidates")

	// 1. 视频 A：有效曝光 200，3 赞 0 评 -> 互动分 = 7/220 ≈ 3.18% >= 3% -> 跃迁至 Level 2，发送喜报，移出 ZSet
	const videoIDA = uint(88001)
	vA := video.Video{
		ID:         videoIDA,
		AuthorID:   101,
		Title:      "爆款视频A",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: time.Now().Add(-2 * time.Hour),
		LikesCount: 3,
	}
	db.Where("id = ?", videoIDA).Delete(&video.Video{})
	if err := db.Create(&vA).Error; err != nil {
		t.Fatalf("create vA failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", videoIDA).Delete(&video.Video{}) })

	// Redis 注入候选池
	mr.ZAdd(candidatesKey, 200, strconv.FormatUint(uint64(videoIDA), 10))

	resA, err := worker.SettleVideo(ctx, videoIDA)
	if err != nil {
		t.Fatalf("SettleVideo(vA) failed: %v", err)
	}
	if !resA.CasSucceeded {
		t.Errorf("vA CasSucceeded = false, want true")
	}
	if resA.TargetPoolLevel != video.PoolLevelAdvanced {
		t.Errorf("vA TargetPoolLevel = %d, want %d", resA.TargetPoolLevel, video.PoolLevelAdvanced)
	}
	if !resA.Promoted {
		t.Errorf("vA Promoted = false, want true")
	}

	// 验证 DB 状态跃迁为 2
	var checkA video.Video
	db.First(&checkA, videoIDA)
	if checkA.PoolLevel != video.PoolLevelAdvanced {
		t.Errorf("DB vA PoolLevel = %d, want %d", checkA.PoolLevel, video.PoolLevelAdvanced)
	}

	// 验证向作者 101 发送了喜报
	if broadcaster.Count() != 1 {
		t.Errorf("broadcaster.Count = %d, want 1", broadcaster.Count())
	}
	if broadcaster.calls[0].UserID != 101 || broadcaster.calls[0].TargetID != videoIDA {
		t.Errorf("broadcaster call mismatch: %+v", broadcaster.calls[0])
	}

	// 验证从 Redis ZSet 移出
	scoreA, err := mr.ZScore(candidatesKey, strconv.FormatUint(uint64(videoIDA), 10))
	if err == nil || scoreA != 0 {
		t.Errorf("video A should be removed from Redis candidates ZSet, got score %f", scoreA)
	}

	// 2. 视频 B：有效曝光 200，0 赞 0 评 -> 互动分 = 1/220 ≈ 0.45% < 0.8% -> 沉底淘汰至 Level 0，不发喜报，移出 ZSet
	const videoIDB = uint(88002)
	vB := video.Video{
		ID:         videoIDB,
		AuthorID:   102,
		Title:      "低质视频B",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: time.Now().Add(-2 * time.Hour),
		LikesCount: 0,
	}
	db.Where("id = ?", videoIDB).Delete(&video.Video{})
	if err := db.Create(&vB).Error; err != nil {
		t.Fatalf("create vB failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", videoIDB).Delete(&video.Video{}) })

	mr.ZAdd(candidatesKey, 200, strconv.FormatUint(uint64(videoIDB), 10))

	resB, err := worker.SettleVideo(ctx, videoIDB)
	if err != nil {
		t.Fatalf("SettleVideo(vB) failed: %v", err)
	}
	if !resB.CasSucceeded {
		t.Errorf("vB CasSucceeded = false, want true")
	}
	if resB.TargetPoolLevel != video.PoolLevelSunk {
		t.Errorf("vB TargetPoolLevel = %d, want %d", resB.TargetPoolLevel, video.PoolLevelSunk)
	}
	if resB.Promoted {
		t.Errorf("vB Promoted = true, want false")
	}

	// 验证 DB 状态更新为 0 (Sunk)
	var checkB video.Video
	db.First(&checkB, videoIDB)
	if checkB.PoolLevel != video.PoolLevelSunk {
		t.Errorf("DB vB PoolLevel = %d, want %d", checkB.PoolLevel, video.PoolLevelSunk)
	}

	// 验证绝未向作者 102 发送喜报 (依然只有之前视频 A 的 1 次)
	if broadcaster.Count() != 1 {
		t.Errorf("broadcaster.Count = %d, want 1 (视频B沉底绝不发喜报)", broadcaster.Count())
	}

	// 验证已从 Redis 移出
	scoreB, errB := mr.ZScore(candidatesKey, strconv.FormatUint(uint64(videoIDB), 10))
	if errB == nil || scoreB != 0 {
		t.Errorf("video B should be removed from Redis candidates ZSet, got score %f", scoreB)
	}

	// 3. 视频 C：有效曝光 120 (< 200)，未满 24 小时 -> 试水进行中，保留在种子池继续积累曝光
	const videoIDC = uint(88003)
	vC := video.Video{
		ID:         videoIDC,
		AuthorID:   103,
		Title:      "潜力观察视频C",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: time.Now().Add(-1 * time.Hour), // 仅 1 小时
		LikesCount: 1,
	}
	db.Where("id = ?", videoIDC).Delete(&video.Video{})
	if err := db.Create(&vC).Error; err != nil {
		t.Fatalf("create vC failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", videoIDC).Delete(&video.Video{}) })

	mr.ZAdd(candidatesKey, 120, strconv.FormatUint(uint64(videoIDC), 10))

	resC, err := worker.SettleVideo(ctx, videoIDC)
	if err != nil {
		t.Fatalf("SettleVideo(vC) failed: %v", err)
	}
	// 试水期未达标不触发 CAS
	if resC.CasSucceeded {
		t.Errorf("vC CasSucceeded should be false during observation window")
	}

	var checkC video.Video
	db.First(&checkC, videoIDC)
	if checkC.PoolLevel != video.PoolLevelSeed {
		t.Errorf("DB vC PoolLevel = %d, want %d (仍保留在种子池)", checkC.PoolLevel, video.PoolLevelSeed)
	}

	// 4. 视频 D：有效曝光 200 (满配额)，1 赞 0 评 -> 互动分 = 3/220 ≈ 1.36% (介于 0.8% 与 3% 之间，未满 24 小时) -> 仍在探索观察期，保留在种子池继续赛马观察
	const videoIDD = uint(88004)
	vD := video.Video{
		ID:         videoIDD,
		AuthorID:   104,
		Title:      "观察期继续试水视频D",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: time.Now().Add(-2 * time.Hour),
		LikesCount: 1,
	}
	db.Where("id = ?", videoIDD).Delete(&video.Video{})
	if err := db.Create(&vD).Error; err != nil {
		t.Fatalf("create vD failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", videoIDD).Delete(&video.Video{}) })

	mr.ZAdd(candidatesKey, 200, strconv.FormatUint(uint64(videoIDD), 10))

	resD, err := worker.SettleVideo(ctx, videoIDD)
	if err != nil {
		t.Fatalf("SettleVideo(vD) failed: %v", err)
	}
	if resD.CasSucceeded || resD.TargetPoolLevel != video.PoolLevelSeed || resD.Reason != "in_observation_window" {
		t.Errorf("vD should remain in observation window (Level 1) when score in [0.8%%, 3%%) and age < 24h: %+v", resD)
	}
	var checkD video.Video
	db.First(&checkD, videoIDD)
	if checkD.PoolLevel != video.PoolLevelSeed {
		t.Errorf("DB vD PoolLevel = %d, want %d (仍保留在种子池)", checkD.PoolLevel, video.PoolLevelSeed)
	}
	// 验证仍在候选队列
	if score, err := mr.ZScore(candidatesKey, strconv.FormatUint(uint64(videoIDD), 10)); err != nil || score != 200 {
		t.Errorf("video D should remain in Redis candidates ZSet with score 200, but got score=%f, err=%v", score, err)
	}
}

// ---------- 24 小时兜底裁决测试 (杜绝长尾死锁) ----------

func TestTrafficPoolWorker_24hDeadlineFallback(t *testing.T) {
	db := openWorkerIntegrationDB(t)
	cache, mr := setupTestRedis(t)
	broadcaster := &mockBroadcaster{}

	worker := NewTrafficPoolWorker(db, cache, broadcaster)
	ctx := context.Background()
	candidatesKey := cache.Key("feed:racing:candidates")

	now := time.Now()
	worker.WithClock(func() time.Time { return now })

	// 场景 1：发布满 25 小时，曝光仅 30 (< 50) -> 低潜直接温和沉底 (Level 0)
	const vidLowExpose = uint(88101)
	vLow := video.Video{
		ID:         vidLowExpose,
		AuthorID:   201,
		Title:      "低潜曝光不足视频",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: now.Add(-25 * time.Hour),
		LikesCount: 0,
	}
	db.Where("id = ?", vidLowExpose).Delete(&video.Video{})
	if err := db.Create(&vLow).Error; err != nil {
		t.Fatalf("create vLow failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", vidLowExpose).Delete(&video.Video{}) })
	mr.ZAdd(candidatesKey, 30, strconv.FormatUint(uint64(vidLowExpose), 10))

	res1, err := worker.SettleVideo(ctx, vidLowExpose)
	if err != nil {
		t.Fatalf("settle vLow failed: %v", err)
	}
	if !res1.CasSucceeded || res1.TargetPoolLevel != video.PoolLevelSunk {
		t.Errorf("vLow should sink to Level 0: %+v", res1)
	}

	// 场景 2：发布满 25 小时，曝光 80 (>= 50)，但互动分不及 3% -> 24h 低潜沉底 (Level 0)
	const vidLowScore = uint(88102)
	vLowScore := video.Video{
		ID:         vidLowScore,
		AuthorID:   202,
		Title:      "兜底低分视频",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: now.Add(-25 * time.Hour),
		LikesCount: 0, // Score = 1 / 100 = 1% < 3%
	}
	db.Where("id = ?", vidLowScore).Delete(&video.Video{})
	if err := db.Create(&vLowScore).Error; err != nil {
		t.Fatalf("create vLowScore failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", vidLowScore).Delete(&video.Video{}) })
	mr.ZAdd(candidatesKey, 80, strconv.FormatUint(uint64(vidLowScore), 10))

	res2, err := worker.SettleVideo(ctx, vidLowScore)
	if err != nil {
		t.Fatalf("settle vLowScore failed: %v", err)
	}
	if !res2.CasSucceeded || res2.TargetPoolLevel != video.PoolLevelSunk {
		t.Errorf("vLowScore should sink to Level 0: %+v", res2)
	}

	// 场景 3：发布满 25 小时，曝光 80 (>= 50)，点赞 2 (Score = (4+1)/100 = 5% >= 3%) -> 兜底晋升 (Level 2)，并触发喜报
	const vidHigh = uint(88103)
	vHigh := video.Video{
		ID:         vidHigh,
		AuthorID:   203,
		Title:      "冷门优质视频",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: now.Add(-25 * time.Hour),
		LikesCount: 2,
	}
	db.Where("id = ?", vidHigh).Delete(&video.Video{})
	if err := db.Create(&vHigh).Error; err != nil {
		t.Fatalf("create vHigh failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", vidHigh).Delete(&video.Video{}) })
	mr.ZAdd(candidatesKey, 80, strconv.FormatUint(uint64(vidHigh), 10))

	res3, err := worker.SettleVideo(ctx, vidHigh)
	if err != nil {
		t.Fatalf("settle vHigh failed: %v", err)
	}
	if !res3.CasSucceeded || res3.TargetPoolLevel != video.PoolLevelAdvanced || !res3.Promoted {
		t.Errorf("vHigh should promote to Level 2: %+v", res3)
	}
	if broadcaster.Count() != 1 || broadcaster.calls[0].UserID != 203 {
		t.Errorf("vHigh should trigger promotion broadcast to author 203")
	}
}

// ---------- SettleBatch 自动从 Redis ZSet 提取达标/兜底候选视频 ----------

func TestTrafficPoolWorker_SettleBatch_RedisDiscovery(t *testing.T) {
	db := openWorkerIntegrationDB(t)
	cache, mr := setupTestRedis(t)
	broadcaster := &mockBroadcaster{}

	worker := NewTrafficPoolWorker(db, cache, broadcaster)
	ctx := context.Background()
	candidatesKey := cache.Key("feed:racing:candidates")

	now := time.Now()
	worker.WithClock(func() time.Time { return now })

	// 准备三条视频：
	// 1. 达标曝光 210，满分，应晋升 (Level 2)
	const vid1 = uint(88201)
	v1 := video.Video{ID: vid1, AuthorID: 301, Title: "批量视频1", Status: video.VideoStatusPublished, PoolLevel: 1, CreateTime: now.Add(-2 * time.Hour), LikesCount: 5}
	// 2. 到期 26 小时，曝光 30，应沉底 (Level 0)
	const vid2 = uint(88202)
	v2 := video.Video{ID: vid2, AuthorID: 302, Title: "批量视频2", Status: video.VideoStatusPublished, PoolLevel: 1, CreateTime: now.Add(-26 * time.Hour), LikesCount: 0}
	// 3. 曝光 100，发布仅 3 小时，未到期未达标，应跳过保留
	const vid3 = uint(88203)
	v3 := video.Video{ID: vid3, AuthorID: 303, Title: "批量视频3", Status: video.VideoStatusPublished, PoolLevel: 1, CreateTime: now.Add(-3 * time.Hour), LikesCount: 1}

	db.Where("id IN (?, ?, ?)", vid1, vid2, vid3).Delete(&video.Video{})
	_ = db.Create(&v1)
	_ = db.Create(&v2)
	_ = db.Create(&v3)
	t.Cleanup(func() { db.Where("id IN (?, ?, ?)", vid1, vid2, vid3).Delete(&video.Video{}) })

	mr.ZAdd(candidatesKey, 210, strconv.FormatUint(uint64(vid1), 10))
	mr.ZAdd(candidatesKey, 30, strconv.FormatUint(uint64(vid2), 10))
	mr.ZAdd(candidatesKey, 100, strconv.FormatUint(uint64(vid3), 10))

	settledCount, err := worker.SettleBatch(ctx)
	if err != nil {
		t.Fatalf("SettleBatch failed: %v", err)
	}

	if settledCount != 2 {
		t.Errorf("settledCount = %d, want 2 (vid1 and vid2)", settledCount)
	}

	// 验证 vid1 与 vid2 已从候选队列移除，而 vid3 依然保留在候选队列
	if score, err := mr.ZScore(candidatesKey, strconv.FormatUint(uint64(vid1), 10)); err == nil && score != 0 {
		t.Errorf("vid1 should be removed from ZSet")
	}
	if score, err := mr.ZScore(candidatesKey, strconv.FormatUint(uint64(vid2), 10)); err == nil && score != 0 {
		t.Errorf("vid2 should be removed from ZSet")
	}
	if score, err := mr.ZScore(candidatesKey, strconv.FormatUint(uint64(vid3), 10)); err != nil || score != 100 {
		t.Errorf("vid3 should remain in ZSet with score 100, got %f, err=%v", score, err)
	}
}

// ---------- Seam 4: 10 协程并发 CAS 竞争测试 (零状态覆盖、零重复 SSE 推送) ----------

func TestTrafficPoolWorker_ConcurrentCASContention(t *testing.T) {
	db := openWorkerIntegrationDB(t)
	cache, mr := setupTestRedis(t)
	broadcaster := &mockBroadcaster{}

	worker := NewTrafficPoolWorker(db, cache, broadcaster)
	ctx := context.Background()
	candidatesKey := cache.Key("feed:racing:candidates")

	const targetVideoID = uint(88301)
	const authorID = uint(999)

	v := video.Video{
		ID:         targetVideoID,
		AuthorID:   authorID,
		Title:      "并发争抢爆款视频",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed, // 1
		CreateTime: time.Now().Add(-5 * time.Hour),
		LikesCount: 10,
	}

	db.Where("id = ?", targetVideoID).Delete(&video.Video{})
	if err := db.Create(&v).Error; err != nil {
		t.Fatalf("create target video failed: %v", err)
	}
	t.Cleanup(func() { db.Where("id = ?", targetVideoID).Delete(&video.Video{}) })

	mr.ZAdd(candidatesKey, 250, strconv.FormatUint(uint64(targetVideoID), 10))

	// 启动 10 个 Goroutine 并发针对同一条视频执行 SettleVideo
	const concurrency = 10
	var wg sync.WaitGroup
	var casSuccessCount int32
	var casFailCount int32

	startGate := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			res, err := worker.SettleVideo(ctx, targetVideoID)
			if err != nil {
				t.Errorf("concurrent settle failed: %v", err)
				return
			}
			if res != nil && res.CasSucceeded {
				atomic.AddInt32(&casSuccessCount, 1)
			} else {
				atomic.AddInt32(&casFailCount, 1)
			}
		}()
	}

	// 鸣枪起跑
	close(startGate)
	wg.Wait()

	// 核心断言：
	// 1. CAS 成功行数只能有且仅有 1 次生效！
	if casSuccessCount != 1 {
		t.Fatalf("casSuccessCount = %d, want EXACTLY 1 (CAS 必须防范并发重复跃迁)", casSuccessCount)
	}
	if casFailCount != int32(concurrency-1) {
		t.Errorf("casFailCount = %d, want %d", casFailCount, concurrency-1)
	}

	// 2. 喜报 SSE 推送只能有且仅有 1 次广播！
	if broadcaster.Count() != 1 {
		t.Fatalf("broadcaster.Count = %d, want EXACTLY 1 (绝不向创作者发送重复弹窗喜报)", broadcaster.Count())
	}

	// 3. 最终数据库状态必须且仅为 Level 2
	var finalV video.Video
	db.First(&finalV, targetVideoID)
	if finalV.PoolLevel != video.PoolLevelAdvanced {
		t.Errorf("final pool_level = %d, want %d", finalV.PoolLevel, video.PoolLevelAdvanced)
	}

	// 4. Redis 候选队列必须已移除该视频
	score, err := mr.ZScore(candidatesKey, strconv.FormatUint(uint64(targetVideoID), 10))
	if err == nil || score != 0 {
		t.Errorf("candidate video should be removed from Redis, got score %f", score)
	}
}

// ---------- 轮询调度器周期执行与优雅退出测试 ----------

func TestTrafficPoolWorker_RunGracefulShutdown(t *testing.T) {
	worker := NewTrafficPoolWorker(nil, nil, nil).
		WithInterval(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	err := worker.Run(ctx)
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("unexpected error on Run shutdown: %v", err)
	}
}

// ---------- 性能基准：Laplace 评分零内存逃逸 (0 B/op, 0 allocs/op) ----------

func BenchmarkCalculateLaplaceScore(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	var dummy float64
	for i := 0; i < b.N; i++ {
		dummy += CalculateLaplaceScore(int64(i%50), int64(i%20), int64(200+i%100))
	}
	_ = dummy
}

// ---------- 观察期机制与 0 曝光长尾视频 24h 兜底测试 ----------

func TestTrafficPoolWorker_ObservationWindowAndZeroExposureFallback(t *testing.T) {
	db := openWorkerIntegrationDB(t)
	cache, mr := setupTestRedis(t)
	broadcaster := &mockBroadcaster{}

	worker := NewTrafficPoolWorker(db, cache, broadcaster)
	ctx := context.Background()
	candidatesKey := cache.Key("feed:racing:candidates")

	baseTime := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

	t.Run("0.8%~3.0% 观察期在 24h 前后状态跃迁对比", func(t *testing.T) {
		const vidObs = uint(88501)
		vObs := video.Video{
			ID:         vidObs,
			AuthorID:   501,
			Title:      "观察期视频",
			Status:     video.VideoStatusPublished,
			PoolLevel:  video.PoolLevelSeed,
			CreateTime: baseTime,
			LikesCount: 1, // Score = 3 / 220 = 1.36% (0.8% <= Score < 3.0%)
		}
		db.Where("id = ?", vidObs).Delete(&video.Video{})
		if err := db.Create(&vObs).Error; err != nil {
			t.Fatalf("create vObs failed: %v", err)
		}
		t.Cleanup(func() { db.Where("id = ?", vidObs).Delete(&video.Video{}) })

		mr.ZAdd(candidatesKey, 200, strconv.FormatUint(uint64(vidObs), 10))

		// 阶段 1: 发布仅 2 小时 (未到期)，曝光达标 200 次 -> 必须保留在种子池观察期
		worker.WithClock(func() time.Time { return baseTime.Add(2 * time.Hour) })
		resEarly, err := worker.SettleVideo(ctx, vidObs)
		if err != nil {
			t.Fatalf("early settle failed: %v", err)
		}
		if resEarly.CasSucceeded || resEarly.TargetPoolLevel != video.PoolLevelSeed || resEarly.Reason != "in_observation_window" {
			t.Fatalf("expected observation window at 2h, got: %+v", resEarly)
		}

		// 阶段 2: 发布满 25 小时 (已到期)，曝光 200 次，平滑分 1.36% 未达到 3% -> 兜底沉底
		worker.WithClock(func() time.Time { return baseTime.Add(25 * time.Hour) })
		resLate, err := worker.SettleVideo(ctx, vidObs)
		if err != nil {
			t.Fatalf("late settle failed: %v", err)
		}
		if !resLate.CasSucceeded || resLate.TargetPoolLevel != video.PoolLevelSunk || resLate.Reason != "deadline_low_potential" {
			t.Fatalf("expected deadline low potential sink at 25h, got: %+v", resLate)
		}

		// 验证 DB 更新为 PoolLevelSunk
		var check video.Video
		db.First(&check, vidObs)
		if check.PoolLevel != video.PoolLevelSunk {
			t.Fatalf("expected DB pool_level = %d, got %d", video.PoolLevelSunk, check.PoolLevel)
		}
	})

	t.Run("0 曝光长尾视频在未入 Redis 时通过 SettleBatch 兜底巡检沉底", func(t *testing.T) {
		const vidZero = uint(88502)
		vZero := video.Video{
			ID:         vidZero,
			AuthorID:   502,
			Title:      "0曝光长尾视频",
			Status:     video.VideoStatusPublished,
			PoolLevel:  video.PoolLevelSeed,
			CreateTime: baseTime,
			LikesCount: 0,
		}
		db.Where("id = ?", vidZero).Delete(&video.Video{})
		if err := db.Create(&vZero).Error; err != nil {
			t.Fatalf("create vZero failed: %v", err)
		}
		t.Cleanup(func() { db.Where("id = ?", vidZero).Delete(&video.Video{}) })

		// 注意：故意不将该视频加入 Redis ZSet (模拟 0 曝光视频未触发写入)
		worker.WithClock(func() time.Time { return baseTime.Add(25 * time.Hour) })

		settledCount, err := worker.SettleBatch(ctx)
		if err != nil {
			t.Fatalf("SettleBatch failed: %v", err)
		}
		if settledCount < 1 {
			t.Fatalf("expected at least 1 settled video from 24h DB sweep, got %d", settledCount)
		}

		var checkZero video.Video
		db.First(&checkZero, vidZero)
		if checkZero.PoolLevel != video.PoolLevelSunk {
			t.Fatalf("expected 0-exposure video to sink to %d, got %d", video.PoolLevelSunk, checkZero.PoolLevel)
		}
	})
}


