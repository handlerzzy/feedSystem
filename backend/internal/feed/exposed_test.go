package feed

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	rediscache "github.com/handlerzzy/feedsystem/internal/middleware/redis"
	"github.com/handlerzzy/feedsystem/internal/video"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// ---------- Ticket 04: 基于 Redis 的短时滑动曝光过滤集合 (Exposed Filter) ----------

func exposedTestKey(accountID uint) string {
	return fmt.Sprintf("p2test:feed:exposed:%d", accountID)
}

func TestRecordExposedAndSlidingWindow(t *testing.T) {
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
	accountID := uint(7)
	key := exposedTestKey(accountID)

	// 1. 记录第 1 页曝光
	page1IDs := []uint{101, 102, 103}
	if err := svc.RecordExposed(ctx, accountID, page1IDs); err != nil {
		t.Fatalf("RecordExposed page 1 failed: %v", err)
	}

	members, err := mr.Members(key)
	if err != nil {
		t.Fatalf("read exposed members: %v", err)
	}
	sort.Strings(members)
	wantMembers := []string{"101", "102", "103"}
	if !reflect.DeepEqual(members, wantMembers) {
		t.Fatalf("got members %v, want %v", members, wantMembers)
	}

	ttl := mr.TTL(key)
	if ttl <= 0 || ttl > DefaultExposedFilterTTL {
		t.Fatalf("expected ttl in (0, %v], got %v", DefaultExposedFilterTTL, ttl)
	}

	// 2. 模拟经过 10 分钟后追加第 2 页曝光（验证滑动过期时间刷新）
	mr.FastForward(10 * time.Minute)
	ttlBefore := mr.TTL(key)
	if ttlBefore > 21*time.Minute {
		t.Fatalf("expected ttl to decrease after 10m, got %v", ttlBefore)
	}

	page2IDs := []uint{104, 105}
	if err := svc.RecordExposed(ctx, accountID, page2IDs); err != nil {
		t.Fatalf("RecordExposed page 2 failed: %v", err)
	}

	members, err = mr.Members(key)
	if err != nil {
		t.Fatalf("read members after page 2: %v", err)
	}
	sort.Strings(members)
	wantAll := []string{"101", "102", "103", "104", "105"}
	if !reflect.DeepEqual(members, wantAll) {
		t.Fatalf("got cumulative members %v, want %v", members, wantAll)
	}

	// 滑动 TTL 刷新至接近 30 分钟
	ttlAfter := mr.TTL(key)
	if ttlAfter <= 25*time.Minute {
		t.Fatalf("expected sliding TTL to be refreshed > 25m, got %v", ttlAfter)
	}

	// 验证 getExposedFilterSet 正确解析
	filterSet := svc.getExposedFilterSet(ctx, accountID)
	if len(filterSet) != 5 {
		t.Fatalf("expected 5 items in filter set, got %d", len(filterSet))
	}
	for _, id := range wantAll {
		var uid uint
		fmt.Sscanf(id, "%d", &uid)
		if !filterSet[uid] {
			t.Errorf("expected video %d to be in filter set", uid)
		}
	}

	// 3. 自然过期重置测试
	mr.FastForward(31 * time.Minute)
	if mr.Exists(key) {
		t.Fatalf("expected key %s to expire after 31m", key)
	}
	expiredSet := svc.getExposedFilterSet(ctx, accountID)
	if len(expiredSet) != 0 {
		t.Fatalf("expected empty set after natural expiration, got %v", expiredSet)
	}
}

func TestSemanticRecallFiltersExposedAndPromotesNextBest(t *testing.T) {
	// 用户点赞了 40（向量 [1, 0]）
	// 候选池中有：
	// - 视频 40: 向量 [1, 0]，相似度 1.0（最优先）
	// - 视频 50: 向量 [0.8, 0.6]，相似度 0.8（次优）
	// - 视频 60: 向量 [0.6, 0.8]，相似度 0.6（第三优）
	store := &fakeVectorStore{
		liked: []uint{40},
		vectors: map[uint][]float32{
			40: {1, 0},
			50: {0.8, 0.6},
			60: {0.6, 0.8},
		},
		candidates: []video.VectorCandidate{
			{ID: 40},
			{ID: 50},
			{ID: 60},
		},
	}
	svc, repo := newSemanticService(t, store, 0.5)
	repo.existing = map[uint]bool{40: true, 50: true, 60: true}

	ctx := context.Background()
	viewerID := uint(7)

	// 1. 未曝光时：首位是视频 40，其次是 50
	hits := svc.semanticRecall(ctx, viewerID, QuotaPlan{Semantic: 2})
	if len(hits) < 2 {
		t.Fatalf("expected at least 2 hits, got %d", len(hits))
	}
	if hits[0].Video.ID != 40 || hits[1].Video.ID != 50 {
		t.Fatalf("initial recall order: got [%d, %d], want [40, 50]", hits[0].Video.ID, hits[1].Video.ID)
	}

	// 2. 将视频 40 写入曝光集合
	if err := svc.RecordExposed(ctx, viewerID, []uint{40}); err != nil {
		t.Fatalf("RecordExposed failed: %v", err)
	}

	// 3. 再次召回：视频 40 必须被剔除，次优视频 50 上升到首位，60 紧随其后
	hitsAfter := svc.semanticRecall(ctx, viewerID, QuotaPlan{Semantic: 2})
	if len(hitsAfter) != 2 {
		t.Fatalf("expected 2 hits after filtering, got %d", len(hitsAfter))
	}
	if hitsAfter[0].Video.ID != 50 {
		t.Fatalf("expected top hit to become next-best video 50, got %d (score=%v)",
			hitsAfter[0].Video.ID, hitsAfter[0].Score)
	}
	if hitsAfter[1].Video.ID != 60 {
		t.Fatalf("expected second hit to be video 60, got %d", hitsAfter[1].Video.ID)
	}
	for _, h := range hitsAfter {
		if h.Video.ID == 40 {
			t.Fatalf("exposed video 40 MUST NOT appear in recall results")
		}
	}
}

// fakeListStore 支持可配置的 ListLatest 返回列表与精确的实体映射
type fakeListStore struct {
	fakeFeedStore
	videosByID     map[uint]*video.Video
	listLatestFunc func(limit int, latestBefore time.Time) []*video.Video
}

func (s *fakeListStore) GetByIDs(_ context.Context, ids []uint) ([]*video.Video, error) {
	out := make([]*video.Video, 0, len(ids))
	for _, id := range ids {
		if v, ok := s.videosByID[id]; ok {
			out = append(out, v)
		} else {
			out = append(out, semanticVideo(id))
		}
	}
	return out, nil
}

func (s *fakeListStore) ListLatest(_ context.Context, limit int, latestBefore time.Time) ([]*video.Video, error) {
	if s.listLatestFunc != nil {
		return s.listLatestFunc(limit, latestBefore), nil
	}
	return s.fakeFeedStore.ListLatest(context.Background(), limit, latestBefore)
}

func TestConsecutivePaginationEliminatesCrossPageDuplicateRecommendations(t *testing.T) {
	// 完整模拟连续向下滑动翻页链路：
	// Page 1: 获得包含语义视频 A 的 Feed 列表，当前页视频 ID 异步写入 Redis 曝光集合；
	// Page 2: 携带 latestBefore 游标请求下一页，语义配额位绝不再推荐视频 A，而是顺利填充次优语义视频 B。
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	baseTime := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	v3 := semanticVideoAt(3, baseTime.Add(3*time.Minute))
	v2 := semanticVideoAt(2, baseTime.Add(2*time.Minute))
	v1 := semanticVideoAt(1, baseTime.Add(1*time.Minute))

	// 向量空间与候选：
	// 语义高分视频 40 (A, score 1.0) 和 次高分视频 50 (B, score 0.8)
	pool := video.NewVectorPool(100, 2)
	pool.PutBatch([]video.VectorItem{
		{VideoID: 40, CreateTime: baseTime.Add(-10 * time.Minute), Vector: []float32{1, 0}},
		{VideoID: 50, CreateTime: baseTime.Add(-20 * time.Minute), Vector: []float32{0.8, 0.6}},
	})

	vectorStore := &fakeVectorStore{
		liked: []uint{40},
	}

	vCold1 := semanticVideoAt(4, baseTime.Add(-5*time.Minute))
	vCold2 := semanticVideoAt(5, baseTime.Add(-6*time.Minute))
	vCold3 := semanticVideoAt(6, baseTime.Add(-7*time.Minute))
	vCold4 := semanticVideoAt(7, baseTime.Add(-8*time.Minute))
	v40 := semanticVideoAt(40, baseTime.Add(-10*time.Minute))
	v50 := semanticVideoAt(50, baseTime.Add(-20*time.Minute))

	feedStore := &fakeListStore{}
	feedStore.existing = map[uint]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 40: true, 50: true}
	feedStore.videosByID = map[uint]*video.Video{
		1:  v1,
		2:  v2,
		3:  v3,
		4:  vCold1,
		5:  vCold2,
		6:  vCold3,
		7:  vCold4,
		40: v40,
		50: v50,
	}
	feedStore.listLatestFunc = func(limit int, latestBefore time.Time) []*video.Video {
		if latestBefore.IsZero() {
			return []*video.Video{v3, v2, v1}
		}
		if latestBefore.After(vCold1.CreateTime) {
			return []*video.Video{vCold1, vCold2, vCold3, vCold4}
		}
		return []*video.Video{}
	}

	cacheClient := rediscache.NewClient(rdb, "p2test:")
	svc := NewFeedService(feedStore, &fakeLikeLookup{}, cacheClient).
		WithSemanticRecall(SemanticRecallOptions{
			Store:      vectorStore,
			VectorPool: pool,
			Model:      "test-model",
			Dim:        2,
			Quota:      0.5, // 50% 配额
		})

	viewerID := uint(7)

	// 记录异步写入曝光集合完成信号
	exposedDone := make(chan struct{}, 10)
	svc.onExposedRecorded = func(uid uint, ids []uint) {
		exposedDone <- struct{}{}
	}

	ctx := context.Background()

	// ===== 请求第 1 页 (limit=4, 槽位：0:Base, 1:Base, 2:Semantic, 3:Base) =====
	resp1, err := svc.ListLatest(ctx, 4, time.Time{}, viewerID)
	if err != nil {
		t.Fatalf("ListLatest page 1 failed: %v", err)
	}

	// 等待第 1 页曝光异步写入完成
	select {
	case <-exposedDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for page 1 exposed videos to be recorded")
	}

	page1IDs := make([]uint, 0, len(resp1.VideoList))
	for _, item := range resp1.VideoList {
		page1IDs = append(page1IDs, item.ID)
	}
	// 第 1 页中应该包含语义视频 40（A）
	has40InPage1 := false
	for _, id := range page1IDs {
		if id == 40 {
			has40InPage1 = true
		}
	}
	if !has40InPage1 {
		t.Fatalf("expected page 1 to recommend top semantic video 40, got items: %v", page1IDs)
	}

	// 校验 Redis 中键已存在且包含第 1 页的所有 ID
	expKey := fmt.Sprintf("p2test:feed:exposed:%d", viewerID)
	if !mr.Exists(expKey) {
		t.Fatalf("exposed key %s not found in redis", expKey)
	}
	for _, id := range page1IDs {
		isMem, err := mr.IsMember(expKey, fmt.Sprintf("%d", id))
		if err != nil || !isMem {
			t.Fatalf("expected video %d to be in exposed set", id)
		}
	}

	// ===== 请求第 2 页（带 latestBefore 游标） =====
	cursor := time.UnixMilli(resp1.NextTime)
	resp2, err := svc.ListLatest(ctx, 4, cursor, viewerID)
	if err != nil {
		t.Fatalf("ListLatest page 2 failed: %v", err)
	}

	// 等待第 2 页曝光异步写入完成（确保连接干净关闭）
	select {
	case <-exposedDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for page 2 exposed videos to be recorded")
	}

	page2IDs := make([]uint, 0, len(resp2.VideoList))
	for _, item := range resp2.VideoList {
		page2IDs = append(page2IDs, item.ID)
	}

	// 核心断言：
	// 1. 语义视频 40 绝不再出现于第 2 页！
	for _, id := range page2IDs {
		if id == 40 {
			t.Fatalf("CRITICAL BUG: exposed semantic video 40 repeated on page 2! items=%v", page2IDs)
		}
	}

	// 2. 次优语义视频 50 (B) 成功填充至语义配额槽位！
	has50InPage2 := false
	for _, id := range page2IDs {
		if id == 50 {
			has50InPage2 = true
		}
	}
	if !has50InPage2 {
		t.Fatalf("expected page 2 to smoothly slide to next-best semantic video 50, got items: %v", page2IDs)
	}
}

func TestAnonymousViewerDoesNotRecordExposedSet(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	baseTime := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	v1 := semanticVideoAt(1, baseTime)

	feedStore := &fakeListStore{}
	feedStore.existing = map[uint]bool{1: true}
	feedStore.listLatestFunc = func(limit int, latestBefore time.Time) []*video.Video {
		if latestBefore.IsZero() {
			return []*video.Video{v1}
		}
		return []*video.Video{}
	}

	cacheClient := rediscache.NewClient(rdb, "p2test:")
	svc := NewFeedService(feedStore, &fakeLikeLookup{}, cacheClient)

	resp, err := svc.ListLatest(context.Background(), 1, time.Time{}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.VideoList) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.VideoList))
	}

	// 断言：未登录用户绝对不生成任何 feed:exposed:0 键
	if mr.Exists("p2test:feed:exposed:0") {
		t.Fatal("feed:exposed:0 key should not be created for anonymous viewer")
	}
}
