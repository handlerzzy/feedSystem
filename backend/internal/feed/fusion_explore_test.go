package feed

import (
	"context"
	"testing"
	"time"

	"github.com/handlerzzy/feedsystem/internal/video"
)

// ---------- Seam 2: 探索槽位优先提取 pool_level = 1 种子视频测试 ----------

type fakeExploreStore struct {
	freshVideos []*video.Video
}

func (s *fakeExploreStore) ListLatest(context.Context, int, time.Time) ([]*video.Video, error) { return nil, nil }
func (s *fakeExploreStore) ListLikesCountWithCursor(context.Context, int, *LikesCountCursor) ([]*video.Video, error) { return nil, nil }
func (s *fakeExploreStore) ListByFollowing(context.Context, int, uint, time.Time) ([]*video.Video, error) { return nil, nil }
func (s *fakeExploreStore) ListByPopularity(context.Context, int, int64, time.Time, uint) ([]*video.Video, error) { return nil, nil }
func (s *fakeExploreStore) GetByIDs(context.Context, []uint) ([]*video.Video, error) { return nil, nil }
func (s *fakeExploreStore) ListByTag(context.Context, string, int) ([]*video.Video, error) { return nil, nil }
func (s *fakeExploreStore) ListLongTailByTag(context.Context, string, int, []uint) ([]*video.Video, error) { return nil, nil }
func (s *fakeExploreStore) ListFreshLowEngagement(context.Context, int, time.Duration, int64, uint) ([]*video.Video, error) {
	return s.freshVideos, nil
}

func TestExploreRecallPrioritizesSeedPoolAndFiltersNonPublished(t *testing.T) {
	vSettled := &video.Video{ID: 101, Status: video.VideoStatusPublished, PoolLevel: video.PoolLevelSunk}
	vSeed1 := &video.Video{ID: 102, Status: video.VideoStatusPublished, PoolLevel: video.PoolLevelSeed}
	vAdvanced := &video.Video{ID: 103, Status: video.VideoStatusPublished, PoolLevel: video.PoolLevelAdvanced}
	vSeed2 := &video.Video{ID: 104, Status: video.VideoStatusPublished, PoolLevel: video.PoolLevelSeed}
	vRejected := &video.Video{ID: 105, Status: video.VideoStatusRejected, PoolLevel: video.PoolLevelSunk}
	vAuditing := &video.Video{ID: 106, Status: video.VideoStatusAuditing, PoolLevel: video.PoolLevelSeed}

	// 模拟 repo 返回未按 pool_level 排序且混杂未审核/驳回数据的原始列表
	rawCandidates := []*video.Video{vSettled, vSeed1, vRejected, vAdvanced, vAuditing, vSeed2}

	store := &fakeExploreStore{freshVideos: rawCandidates}
	svc := NewFeedService(store, nil, nil).WithLocalCache(false)

	ctx := context.Background()
	viewerID := uint(42)
	quota := buildQuotaPlan(10, 0, 0.2) // 2 explore slots

	candidates := svc.exploreRecall(ctx, viewerID, quota)

	// 断言 1：非 published 视频（rejected / auditing）被严格剔除
	for _, v := range candidates {
		if v.ID == 105 || v.ID == 106 {
			t.Fatalf("exploreRecall leaked non-published video: ID=%d status=%s", v.ID, v.Status)
		}
	}

	// 断言 2：剩余视频数量应为 4
	if len(candidates) != 4 {
		t.Fatalf("expected 4 candidates, got %d", len(candidates))
	}

	// 断言 3：pool_level = 1 的种子视频必须排在最前面
	if candidates[0].ID != 102 || candidates[0].PoolLevel != video.PoolLevelSeed {
		t.Errorf("candidate[0] should be seed video 102, got ID=%d pool_level=%d", candidates[0].ID, candidates[0].PoolLevel)
	}
	if candidates[1].ID != 104 || candidates[1].PoolLevel != video.PoolLevelSeed {
		t.Errorf("candidate[1] should be seed video 104, got ID=%d pool_level=%d", candidates[1].ID, candidates[1].PoolLevel)
	}

	// 种子视频之后的候选保持其余层级
	if candidates[2].ID != 101 && candidates[3].ID != 103 {
		t.Errorf("remaining candidates should be settled/advanced, got IDs: %v", idsOfVideos(candidates))
	}
}

func TestFusionExploreSlotSeedPoolPriority(t *testing.T) {
	// 验证最终融合列表中，探索槽位优先注入 pool_level = 1 种子视频
	// ExploreSlotOffset = 5, ExploreSlotStride = 10。因此 limit=20 时有 2 个探索位（pos 5 与 pos 15）。
	base := fuseVideos(20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
	plan := buildQuotaPlan(20, 0, 0.1) // 2 探索位 (20 * 0.1 = 2)

	vSettled := &video.Video{ID: 201, Status: video.VideoStatusPublished, PoolLevel: video.PoolLevelSunk}
	vSeed1 := &video.Video{ID: 202, Status: video.VideoStatusPublished, PoolLevel: video.PoolLevelSeed}
	vSeed2 := &video.Video{ID: 203, Status: video.VideoStatusPublished, PoolLevel: video.PoolLevelSeed}

	// 探索输入经过 exploreRecall 优先排序后：[vSeed1, vSeed2, vSettled]
	exploreCandidates := []*video.Video{vSeed1, vSeed2, vSettled}

	final, _ := fuseChannels(fuseInput{
		Base:    base,
		Explore: exploreCandidates,
		Plan:    plan,
	})

	finalIDs := idsOfVideos(final)
	if !containsID(finalIDs, 202) || !containsID(finalIDs, 203) {
		t.Fatalf("expected seed videos 202 and 203 to be placed in explore slots, got final: %v", finalIDs)
	}
	if containsID(finalIDs, 201) {
		t.Fatalf("settled video 201 should not displace seed videos when quota is 2, got: %v", finalIDs)
	}
}
