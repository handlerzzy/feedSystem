package feed

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rediscache "github.com/handlerzzy/feedsystem/internal/middleware/redis"
	"github.com/handlerzzy/feedsystem/internal/video"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// ---------- Ticket 02: 防刷赛马曝光采集与 Redis 活跃候选池 ----------

func racingUsersKey(videoID uint) string {
	return fmt.Sprintf("p2test:feed:video:racing_users:%d", videoID)
}

func racingImpressionsKey(videoID uint) string {
	return fmt.Sprintf("p2test:feed:video:racing_impressions:%d", videoID)
}

func racingCandidatesKey() string {
	return "p2test:" + RacingCandidatesKey
}

func TestRecordRacingImpression_DedupAndCandidatePool(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cacheClient := rediscache.NewClient(rdb, "p2test:")
	svc := NewFeedService(&fakeFeedStore{}, &fakeLikeLookup{}, cacheClient)

	ctx := context.Background()
	accountID := uint(42)
	videoID := uint(1001)

	// 1. 用户首次曝光视频 1001：有效曝光，返回 true
	first, err := svc.RecordRacingImpression(ctx, accountID, videoID)
	if err != nil {
		t.Fatalf("first RecordRacingImpression failed: %v", err)
	}
	if !first {
		t.Fatalf("expected first impression to be true, got false")
	}

	// 验证 Redis Set feed:video:racing_users:1001 记录了该用户
	uKey := racingUsersKey(videoID)
	isMember, err := mr.IsMember(uKey, fmt.Sprintf("%d", accountID))
	if err != nil {
		t.Fatalf("check set member failed: %v", err)
	}
	if !isMember {
		t.Fatalf("expected account %d in set %s", accountID, uKey)
	}

	// 验证 Set TTL 接近 24 小时
	setTTL := mr.TTL(uKey)
	if setTTL <= 0 || setTTL > DefaultRacingUserTTL {
		t.Fatalf("expected TTL in (0, %v], got %v", DefaultRacingUserTTL, setTTL)
	}

	// 验证曝光计数为 1
	impr, err := svc.GetRacingImpressions(ctx, videoID)
	if err != nil {
		t.Fatalf("GetRacingImpressions failed: %v", err)
	}
	if impr != 1 {
		t.Fatalf("expected impressions 1, got %d", impr)
	}

	// 验证候选队列 feed:racing:candidates 的 score 为 1
	score, exists, err := svc.GetCandidateScore(ctx, videoID)
	if err != nil {
		t.Fatalf("GetCandidateScore failed: %v", err)
	}
	if !exists {
		t.Fatalf("expected video %d to exist in candidates pool", videoID)
	}
	if score != 1.0 {
		t.Fatalf("expected candidate score 1.0, got %f", score)
	}

	// 2. 同一用户在 24 小时内重复刷到同一视频：幂等去重，返回 false
	second, err := svc.RecordRacingImpression(ctx, accountID, videoID)
	if err != nil {
		t.Fatalf("second RecordRacingImpression failed: %v", err)
	}
	if second {
		t.Fatalf("expected duplicate impression to be false (deduped), got true")
	}

	// 验证计数与 score 未增加
	imprAfterDup, err := svc.GetRacingImpressions(ctx, videoID)
	if err != nil {
		t.Fatalf("GetRacingImpressions failed: %v", err)
	}
	if imprAfterDup != 1 {
		t.Fatalf("expected impressions to remain 1, got %d", imprAfterDup)
	}

	scoreAfterDup, _, err := svc.GetCandidateScore(ctx, videoID)
	if err != nil {
		t.Fatalf("GetCandidateScore failed: %v", err)
	}
	if scoreAfterDup != 1.0 {
		t.Fatalf("expected candidate score to remain 1.0, got %f", scoreAfterDup)
	}

	// 3. 第二个独立用户曝光同一视频：有效计数，曝光累加至 2，score 升至 2
	accountID2 := uint(99)
	third, err := svc.RecordRacingImpression(ctx, accountID2, videoID)
	if err != nil {
		t.Fatalf("another user RecordRacingImpression failed: %v", err)
	}
	if !third {
		t.Fatalf("expected another user's impression to be true, got false")
	}

	imprUser2, err := svc.GetRacingImpressions(ctx, videoID)
	if err != nil {
		t.Fatalf("GetRacingImpressions failed: %v", err)
	}
	if imprUser2 != 2 {
		t.Fatalf("expected impressions 2, got %d", imprUser2)
	}

	scoreUser2, _, err := svc.GetCandidateScore(ctx, videoID)
	if err != nil {
		t.Fatalf("GetCandidateScore failed: %v", err)
	}
	if scoreUser2 != 2.0 {
		t.Fatalf("expected candidate score 2.0, got %f", scoreUser2)
	}
}

func TestRecordRacingImpressions_PipelineBatch(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cacheClient := rediscache.NewClient(rdb, "p2test:")
	svc := NewFeedService(&fakeFeedStore{}, &fakeLikeLookup{}, cacheClient)

	ctx := context.Background()
	accountID := uint(55)
	videoIDs := []uint{2001, 2002, 2003}

	// 1. 批量记录 3 个新视频曝光
	results, err := svc.RecordRacingImpressions(ctx, accountID, videoIDs)
	if err != nil {
		t.Fatalf("RecordRacingImpressions batch failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r {
			t.Fatalf("expected video %d first impression to be true", videoIDs[i])
		}
	}

	for _, vid := range videoIDs {
		impr, err := svc.GetRacingImpressions(ctx, vid)
		if err != nil {
			t.Fatalf("GetRacingImpressions failed: %v", err)
		}
		if impr != 1 {
			t.Fatalf("expected video %d impressions 1, got %d", vid, impr)
		}

		score, exists, err := svc.GetCandidateScore(ctx, vid)
		if err != nil {
			t.Fatalf("GetCandidateScore failed: %v", err)
		}
		if !exists || score != 1.0 {
			t.Fatalf("expected video %d in candidates with score 1.0, got exists=%v score=%f", vid, exists, score)
		}
	}

	// 2. 混合批次：videoIDs 中的 2001, 2002 为重复曝光，2004 为新视频
	mixedVideoIDs := []uint{2001, 2002, 2004}
	mixedResults, err := svc.RecordRacingImpressions(ctx, accountID, mixedVideoIDs)
	if err != nil {
		t.Fatalf("RecordRacingImpressions mixed batch failed: %v", err)
	}
	if len(mixedResults) != 3 {
		t.Fatalf("expected 3 results, got %d", len(mixedResults))
	}
	if mixedResults[0] != false || mixedResults[1] != false {
		t.Fatalf("expected 2001 and 2002 to be deduplicated (false), got %v", mixedResults)
	}
	if mixedResults[2] != true {
		t.Fatalf("expected 2004 to be true, got %v", mixedResults)
	}

	// 2001 的曝光依然是 1，2004 的曝光是 1
	impr2001, _ := svc.GetRacingImpressions(ctx, 2001)
	if impr2001 != 1 {
		t.Fatalf("expected 2001 impressions to remain 1, got %d", impr2001)
	}
	impr2004, _ := svc.GetRacingImpressions(ctx, 2004)
	if impr2004 != 1 {
		t.Fatalf("expected 2004 impressions 1, got %d", impr2004)
	}
}

func TestRecordRacingImpression_ConcurrentAntiCheat(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cacheClient := rediscache.NewClient(rdb, "p2test:")
	svc := NewFeedService(&fakeFeedStore{}, &fakeLikeLookup{}, cacheClient)

	ctx := context.Background()
	accountID := uint(88)
	videoID := uint(3001)

	// 50 个 Goroutine 模拟同一用户在毫秒级内高频并发刷同一视频（模拟爬虫或狂刷）
	const concurrency = 50
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var dedupCount atomic.Int64

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			effective, err := svc.RecordRacingImpression(ctx, accountID, videoID)
			if err != nil {
				t.Errorf("concurrent RecordRacingImpression error: %v", err)
				return
			}
			if effective {
				successCount.Add(1)
			} else {
				dedupCount.Add(1)
			}
		}()
	}
	wg.Wait()

	// 必须严格有且仅有 1 次生效，其余 49 次全部判定为去重
	if successCount.Load() != 1 {
		t.Fatalf("expected exactly 1 effective impression, got %d", successCount.Load())
	}
	if dedupCount.Load() != concurrency-1 {
		t.Fatalf("expected %d deduplications, got %d", concurrency-1, dedupCount.Load())
	}

	// 检查曝光数与候选池 score 均为 1
	impr, err := svc.GetRacingImpressions(ctx, videoID)
	if err != nil {
		t.Fatalf("GetRacingImpressions failed: %v", err)
	}
	if impr != 1 {
		t.Fatalf("expected impressions 1, got %d", impr)
	}

	score, exists, err := svc.GetCandidateScore(ctx, videoID)
	if err != nil {
		t.Fatalf("GetCandidateScore failed: %v", err)
	}
	if !exists || score != 1.0 {
		t.Fatalf("expected candidate score 1.0, got exists=%v score=%f", exists, score)
	}
}

func TestRecordRacingImpression_ConcurrentMultiUsers(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cacheClient := rediscache.NewClient(rdb, "p2test:")
	svc := NewFeedService(&fakeFeedStore{}, &fakeLikeLookup{}, cacheClient)

	ctx := context.Background()
	videoID := uint(4001)

	// 50 个不同用户并发刷视频 4001
	const userCount = 50
	var wg sync.WaitGroup
	var effectiveCount atomic.Int64

	wg.Add(userCount)
	for i := 1; i <= userCount; i++ {
		uid := uint(i + 1000)
		go func(accountID uint) {
			defer wg.Done()
			effective, err := svc.RecordRacingImpression(ctx, accountID, videoID)
			if err != nil {
				t.Errorf("RecordRacingImpression error: %v", err)
				return
			}
			if effective {
				effectiveCount.Add(1)
			}
		}(uid)
	}
	wg.Wait()

	// 50 个不同用户应该全部产生有效曝光
	if effectiveCount.Load() != userCount {
		t.Fatalf("expected %d effective impressions, got %d", userCount, effectiveCount.Load())
	}

	impr, err := svc.GetRacingImpressions(ctx, videoID)
	if err != nil {
		t.Fatalf("GetRacingImpressions failed: %v", err)
	}
	if impr != userCount {
		t.Fatalf("expected impressions %d, got %d", userCount, impr)
	}

	score, exists, err := svc.GetCandidateScore(ctx, videoID)
	if err != nil {
		t.Fatalf("GetCandidateScore failed: %v", err)
	}
	if !exists || score != float64(userCount) {
		t.Fatalf("expected candidate score %d.0, got exists=%v score=%f", userCount, exists, score)
	}
}

func TestFeedService_ListLatest_RecordsRacingImpressionsAsync(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cacheClient := rediscache.NewClient(rdb, "p2test:")

	now := time.Now()
	v1 := &video.Video{ID: 5001, Title: "v1", Status: "published", PoolLevel: 1, CreateTime: now}
	v2 := &video.Video{ID: 5002, Title: "v2", Status: "published", PoolLevel: 1, CreateTime: now.Add(-time.Minute)}

	store := &fakeListStore{
		videosByID: map[uint]*video.Video{5001: v1, 5002: v2},
		listLatestFunc: func(limit int, latestBefore time.Time) []*video.Video {
			if latestBefore.IsZero() {
				return []*video.Video{v1, v2}
			}
			return []*video.Video{}
		},
	}
	svc := NewFeedService(store, &fakeLikeLookup{}, cacheClient)

	// 配置回调通知通道，等待异步 goroutine 完成
	racingDone := make(chan []uint, 1)
	svc.onRacingRecorded = func(accountID uint, ids []uint) {
		racingDone <- ids
	}

	exposedDone := make(chan []uint, 1)
	svc.onExposedRecorded = func(accountID uint, ids []uint) {
		exposedDone <- ids
	}

	ctx := context.Background()
	viewerID := uint(777)

	resp, err := svc.ListLatest(ctx, 10, time.Time{}, viewerID)
	if err != nil {
		t.Fatalf("ListLatest failed: %v", err)
	}
	if len(resp.VideoList) != 2 {
		t.Fatalf("expected 2 videos, got %d", len(resp.VideoList))
	}

	// 等待两个独立的异步写入完成（验证完全解耦且各自正常执行）
	select {
	case ids := <-exposedDone:
		if len(ids) != 2 {
			t.Fatalf("expected 2 exposed ids, got %d", len(ids))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for exposed filter async write")
	}

	select {
	case ids := <-racingDone:
		if len(ids) != 2 {
			t.Fatalf("expected 2 racing ids, got %d", len(ids))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for racing impressions async write")
	}

	// 验证曝光计数与候选池
	for _, vid := range []uint{5001, 5002} {
		impr, err := svc.GetRacingImpressions(ctx, vid)
		if err != nil {
			t.Fatalf("GetRacingImpressions error: %v", err)
		}
		if impr != 1 {
			t.Fatalf("expected video %d impressions 1, got %d", vid, impr)
		}
		score, exists, err := svc.GetCandidateScore(ctx, vid)
		if err != nil || !exists || score != 1.0 {
			t.Fatalf("expected candidate score 1.0, got exists=%v score=%f err=%v", exists, score, err)
		}
	}
}


