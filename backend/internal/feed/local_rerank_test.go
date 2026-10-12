package feed

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/handlerzzy/feedsystem/internal/video"
)

// TestCalculateLocalRerankScore 验证本地精排多特征打分公式的准确性。
func TestCalculateLocalRerankScore(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	opts := DefaultLocalRerankOptions()

	// 视频 1：刚刚发布 (diff=0 => timeScore=1.0)，0 赞 (heatScore=0)，相似度 0.8
	v1 := &video.Video{
		ID:         1,
		CreateTime: now,
		LikesCount: 0,
	}
	res1 := CalculateLocalRerankScore(v1, 0.8, now, opts)
	expectedScore1 := 0.6*0.8 + 0.3*1.0 + 0.1*0.0 // 0.48 + 0.30 = 0.78
	if math.Abs(res1.FinalScore-expectedScore1) > 1e-4 {
		t.Errorf("res1 FinalScore = %f, want %f", res1.FinalScore, expectedScore1)
	}
	if math.Abs(res1.TimeScore-1.0) > 1e-4 {
		t.Errorf("res1 TimeScore = %f, want 1.0", res1.TimeScore)
	}
	if math.Abs(res1.HeatScore-0.0) > 1e-4 {
		t.Errorf("res1 HeatScore = %f, want 0.0", res1.HeatScore)
	}

	// 视频 2：发布于 72 小时前 (半衰期 => timeScore=0.5)，10,000 赞 (heatScore=1.0)，相似度 0.5
	v2 := &video.Video{
		ID:         2,
		CreateTime: now.Add(-72 * time.Hour),
		LikesCount: 10000,
	}
	res2 := CalculateLocalRerankScore(v2, 0.5, now, opts)
	expectedScore2 := 0.6*0.5 + 0.3*0.5 + 0.1*1.0 // 0.30 + 0.15 + 0.10 = 0.55
	if math.Abs(res2.FinalScore-expectedScore2) > 1e-4 {
		t.Errorf("res2 FinalScore = %f, want %f", res2.FinalScore, expectedScore2)
	}
	if math.Abs(res2.TimeScore-0.5) > 1e-4 {
		t.Errorf("res2 TimeScore = %f, want 0.5", res2.TimeScore)
	}
	if math.Abs(res2.HeatScore-1.0) > 1e-4 {
		t.Errorf("res2 HeatScore = %f, want 1.0", res2.HeatScore)
	}

	// 异常值防御测试：nil 视频、负点赞数、负相似度、未来发布时间
	resNil := CalculateLocalRerankScore(nil, 0.5, now, opts)
	if resNil.Video != nil || resNil.FinalScore != 0 {
		t.Errorf("expected zero result for nil video, got %+v", resNil)
	}

	vNegative := &video.Video{
		ID:         3,
		CreateTime: now.Add(2 * time.Hour), // 未来发布时间
		LikesCount: -10,                    // 负赞
	}
	resNeg := CalculateLocalRerankScore(vNegative, -0.5, now, opts)
	if resNeg.SemanticSim != 0 {
		t.Errorf("negative sim should be clamped to 0, got %f", resNeg.SemanticSim)
	}
	if resNeg.TimeScore != 1.0 {
		t.Errorf("future create time should have timeScore 1.0, got %f", resNeg.TimeScore)
	}
	if resNeg.HeatScore != 0.0 {
		t.Errorf("negative likes should have heatScore 0.0, got %f", resNeg.HeatScore)
	}
}

// TestLocalRerankVideosOrdering 验证多特征重排对高语义匹配度视频的提拔与排序稳定性。
func TestLocalRerankVideosOrdering(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	opts := DefaultLocalRerankOptions()

	// 视频 A：最新发布 (1 小时前)，但语义不相关 (sim=0)，50 赞
	vA := &video.Video{ID: 10, CreateTime: now.Add(-1 * time.Hour), LikesCount: 50}
	// 视频 B：较早发布 (48 小时前)，高语义相关 (sim=0.95)，200 赞
	vB := &video.Video{ID: 20, CreateTime: now.Add(-48 * time.Hour), LikesCount: 200}
	// 视频 C：更老发布 (120 小时前)，低相关 (sim=0.1)，10 赞
	vC := &video.Video{ID: 30, CreateTime: now.Add(-120 * time.Hour), LikesCount: 10}

	items := []*video.Video{vA, vB, vC}
	sims := map[uint]float64{
		10: 0.0,
		20: 0.95,
		30: 0.1,
	}

	reranked := LocalRerankVideos(items, sims, now, opts)
	if len(reranked) != 3 {
		t.Fatalf("expected 3 items, got %d", len(reranked))
	}

	// 视频 B 凭借极高语义分 (0.95 * 0.6 = 0.57) 应超越最新发布的视频 A 排到第一
	if reranked[0].ID != 20 {
		t.Errorf("expected top video to be 20 (high semantic match), got %d", reranked[0].ID)
	}
	if reranked[1].ID != 10 {
		t.Errorf("expected second video to be 10 (fresh base video), got %d", reranked[1].ID)
	}
	if reranked[2].ID != 30 {
		t.Errorf("expected third video to be 30 (old & low relevance), got %d", reranked[2].ID)
	}
}

// TestLocalRerankVideos_CorrectnessAndHangGuard 验证纯本地轻量特征打分精排的执行正确性与防挂死保障。
// 注：微秒级 SLA 度量 (<100μs) 与 0 堆内存分配门禁已解耦委托给 BenchmarkCalculateLocalRerankScore_50Candidates。
// 单元测试仅保留宽松的防死锁/防挂起熔断阈值与业务正确性校验，彻底消除 CI 虚拟机 CPU 抢占造成的 Flaky 误报。
func TestLocalRerankVideos_CorrectnessAndHangGuard(t *testing.T) {
	now := time.Now()
	opts := DefaultLocalRerankOptions()

	const numCandidates = 50
	items := make([]*video.Video, numCandidates)
	sims := make(map[uint]float64, numCandidates)
	for i := 0; i < numCandidates; i++ {
		items[i] = &video.Video{
			ID:         uint(i + 1),
			CreateTime: now.Add(-time.Duration(i*2) * time.Hour),
			LikesCount: int64(i * 50),
		}
		sims[uint(i+1)] = float64(i%10) / 10.0
	}

	// 验证业务结果正确性：50 条候选重排后长度保持一致且非空
	initialRerank := LocalRerankVideos(items, sims, now, opts)
	if len(initialRerank) != numCandidates {
		t.Fatalf("expected %d reranked items, got %d", numCandidates, len(initialRerank))
	}

	// 宽松的防挂死守护：1,000 次重排应当在 1 秒防死锁安全上限内完成
	const iterations = 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_ = LocalRerankVideos(items, sims, now, opts)
	}
	elapsed := time.Since(start)

	maxAllowedTotal := 1 * time.Second
	if raceEnabled {
		maxAllowedTotal = 5 * time.Second
	}
	if elapsed > maxAllowedTotal {
		t.Fatalf("本地多特征精排执行可能发生死锁或挂起，总耗时 %v 超出安全上限 %v", elapsed, maxAllowedTotal)
	}
}


// TestLocalRerankSemanticHitsColdStartAndEdgeCases 验证无候选或少于 2 条时保持原样。
func TestLocalRerankSemanticHitsColdStartAndEdgeCases(t *testing.T) {
	v1 := &video.Video{ID: 1, Title: "v1"}

	// 1. 语义候选为空 (冷启动新用户)
	gotCold := LocalRerankSemanticHits(nil, time.Now(), DefaultLocalRerankOptions())
	if len(gotCold) != 0 {
		t.Fatalf("冷启动无语义候选时应返回空: %v", gotCold)
	}

	// 2. 候选少于 2 条
	single := []SemanticHit{{Video: v1, Score: 0.9}}
	gotSingle := LocalRerankSemanticHits(single, time.Now(), DefaultLocalRerankOptions())
	if len(gotSingle) != 1 || gotSingle[0].Video.ID != 1 {
		t.Fatalf("单条视频应原样返回: %v", gotSingle)
	}
}

// TestLocalRerankSemanticHits 验证语义命中候选集的多特征打分与排序。
func TestLocalRerankSemanticHits(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	opts := DefaultLocalRerankOptions()

	// 视频 1：新鲜 (1h 前)，高相似度 0.9，50 赞
	v1 := &video.Video{ID: 1, CreateTime: now.Add(-1 * time.Hour), LikesCount: 50}
	// 视频 2：陈旧 (120h 前)，较高相似度 0.95，0 赞 (时间衰减严重)
	v2 := &video.Video{ID: 2, CreateTime: now.Add(-120 * time.Hour), LikesCount: 0}
	// 视频 3：适度新鲜 (24h 前)，相似度 0.8，10,000 赞 (高赞提升)
	v3 := &video.Video{ID: 3, CreateTime: now.Add(-24 * time.Hour), LikesCount: 10000}

	hits := []SemanticHit{
		{Video: v2, Score: 0.95},
		{Video: v1, Score: 0.90},
		{Video: v3, Score: 0.80},
	}

	reranked := LocalRerankSemanticHits(hits, now, opts)
	if len(reranked) != 3 {
		t.Fatalf("expected 3 hits, got %d", len(reranked))
	}

	// 计算预期分：
	// v1: 0.6*0.9 + 0.3*2^(-1/72) + 0.1*(log1p(50)/log1p(10000)) ≈ 0.54 + 0.297 + 0.042 = 0.879
	// v2: 0.6*0.95 + 0.3*2^(-120/72) + 0.1*0 ≈ 0.57 + 0.3*0.315 = 0.664
	// v3: 0.6*0.8 + 0.3*2^(-24/72) + 0.1*1.0 ≈ 0.48 + 0.238 + 0.10 = 0.818
	// 预期排序：v1 > v3 > v2
	if reranked[0].Video.ID != 1 {
		t.Errorf("expected top hit to be v1, got %d", reranked[0].Video.ID)
	}
	if reranked[1].Video.ID != 3 {
		t.Errorf("expected second hit to be v3, got %d", reranked[1].Video.ID)
	}
	if reranked[2].Video.ID != 2 {
		t.Errorf("expected third hit to be v2, got %d", reranked[2].Video.ID)
	}
}

// TestListLatestPreservesQuotaSlotsAndAvoidsSemanticDominance 验证 Task 3:
// 配额槽位融合在 ListLatest 下发时严格保持预定插槽比例（如 30% 配额下语义内容落在指定槽位），
// 本地精排前置在语义通道内部执行，绝不能在融合后全量重排导致语义视频集中霸占首屏。
func TestListLatestPreservesQuotaSlotsAndAvoidsSemanticDominance(t *testing.T) {
	now := time.Now()
	// 准备 10 条基准时序视频 (ID 1~10，最新)
	baseVideos := make([]*video.Video, 10)
	for i := 0; i < 10; i++ {
		id := uint(i + 1)
		baseVideos[i] = &video.Video{
			ID:         id,
			Title:      "base",
			CreateTime: now.Add(-time.Duration(i) * time.Minute),
			LikesCount: 10,
		}
	}

	// 准备 3 条语义视频 (ID 101, 102, 103，相似度极高 0.95)
	semVideos := make([]*video.Video, 3)
	for i := 0; i < 3; i++ {
		id := uint(101 + i)
		semVideos[i] = &video.Video{
			ID:         id,
			Title:      "semantic",
			CreateTime: now.Add(-time.Duration(10+i) * time.Hour),
			LikesCount: 100,
		}
	}

	store := &fakeVectorStore{
		accountID: 7,
		liked:     []uint{999},
		vectors: map[uint][]float32{
			999: {1, 0},
			101: {1, 0},
			102: {0.99, 0.1},
			103: {0.98, 0.2},
		},
	}

	svc, repo := newSemanticService(t, store, 0.3) // 30% 语义配额

	repo.existing = make(map[uint]bool)
	for _, v := range baseVideos {
		repo.existing[v.ID] = true
	}
	for _, v := range semVideos {
		repo.existing[v.ID] = true
	}
	repo.probe = func() []*video.Video {
		return baseVideos
	}

	pool := video.NewVectorPool(20, 2)
	for _, v := range semVideos {
		pool.Put(video.VectorItem{
			VideoID:    v.ID,
			CreateTime: v.CreateTime,
			Vector:     store.vectors[v.ID],
		})
	}
	svc.WithVectorPool(pool)

	resp, err := svc.ListLatest(context.Background(), 10, time.Time{}, 7)
	if err != nil {
		t.Fatalf("ListLatest error: %v", err)
	}
	if len(resp.VideoList) != 10 {
		t.Fatalf("expected 10 videos, got %d", len(resp.VideoList))
	}

	// 验证配额槽位：第 1 位和第 2 位（下标 0, 1）必须是时序基准视频，绝不能被语义视频霸屏挤占！
	if resp.VideoList[0].ID > 10 {
		t.Errorf("第 1 位应该是时序基础视频，实际出现语义视频 ID=%d (全量重排破坏了配额槽位)", resp.VideoList[0].ID)
	}
	if resp.VideoList[1].ID > 10 {
		t.Errorf("第 2 位应该是时序基础视频，实际出现语义视频 ID=%d (全量重排破坏了配额槽位)", resp.VideoList[1].ID)
	}

	// 验证语义视频恰好落在配额插槽（下标 2, 5, 8）
	semanticIDs := map[uint]bool{101: true, 102: true, 103: true}
	for _, slot := range []int{2, 5, 8} {
		if !semanticIDs[resp.VideoList[slot].ID] {
			t.Errorf("槽位 %d 期望为语义召回视频 (101~103)，实际为 ID=%d", slot, resp.VideoList[slot].ID)
		}
	}
}
