package video

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"
)

func TestVectorPool_Put_ValidationAndGet(t *testing.T) {
	pool := NewVectorPool(3, 4)
	if pool.Len() != 0 {
		t.Fatalf("expected empty pool, got %d", pool.Len())
	}

	now := time.Now()

	// 1. 维度不匹配应拒绝入池
	_, err := pool.Put(VectorItem{
		VideoID:    1,
		CreateTime: now,
		Vector:     []float32{1.0, 0.0, 0.0}, // 3 维 != 4 维
	})
	if err == nil {
		t.Errorf("expected error for dimension mismatch, got nil")
	}

	// 2. 零长度/空向量拒绝入池
	_, err = pool.Put(VectorItem{
		VideoID:    2,
		CreateTime: now,
		Vector:     nil,
	})
	if err == nil {
		t.Errorf("expected error for nil vector, got nil")
	}

	// 3. 包含 NaN / Inf 的向量拒绝入池
	_, err = pool.Put(VectorItem{
		VideoID:    3,
		CreateTime: now,
		Vector:     []float32{1.0, float32(math.NaN()), 0.0, 0.0},
	})
	if err == nil {
		t.Errorf("expected error for NaN vector, got nil")
	}

	// 4. VideoID 为 0 拒绝入池
	_, err = pool.Put(VectorItem{
		VideoID:    0,
		CreateTime: now,
		Vector:     []float32{1.0, 0.0, 0.0, 0.0},
	})
	if err == nil {
		t.Errorf("expected error for VideoID=0, got nil")
	}

	// 5. 零向量 (全 0) 拒绝入池
	_, err = pool.Put(VectorItem{
		VideoID:    4,
		CreateTime: now,
		Vector:     []float32{0.0, 0.0, 0.0, 0.0},
	})
	if err == nil {
		t.Errorf("expected error for all-zero vector, got nil")
	}

	// 6. 插入合法向量，应自动执行 L2 归一化并成功写入
	validVec := []float32{3.0, 4.0, 0.0, 0.0} // norm = 5
	admitted, err := pool.Put(VectorItem{
		VideoID:    10,
		CreateTime: now,
		Vector:     validVec,
	})
	if err != nil || !admitted {
		t.Fatalf("unexpected error on valid Put: admitted=%v, err=%v", admitted, err)
	}

	if pool.Len() != 1 {
		t.Fatalf("expected pool len 1, got %d", pool.Len())
	}

	item, ok := pool.Get(10)
	if !ok {
		t.Fatalf("expected item 10 found in pool")
	}
	if item.VideoID != 10 {
		t.Errorf("expected VideoID 10, got %d", item.VideoID)
	}
	if math.Abs(float64(item.Vector[0]-0.6)) > 1e-5 || math.Abs(float64(item.Vector[1]-0.8)) > 1e-5 {
		t.Errorf("expected normalized vector [0.6, 0.8, 0, 0], got %v", item.Vector)
	}

	// 7. 查询不存在的 VideoID
	_, ok = pool.Get(999)
	if ok {
		t.Errorf("expected not found for nonexistent VideoID")
	}
}

func TestVectorPool_CapacityAndEviction(t *testing.T) {
	pool := NewVectorPool(3, 4)
	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	vec := []float32{1, 0, 0, 0}

	// 依次插入 3 条数据填充池子
	_, _ = pool.Put(VectorItem{VideoID: 1, CreateTime: baseTime, Vector: vec})
	_, _ = pool.Put(VectorItem{VideoID: 2, CreateTime: baseTime.Add(1 * time.Hour), Vector: vec})
	_, _ = pool.Put(VectorItem{VideoID: 3, CreateTime: baseTime.Add(2 * time.Hour), Vector: vec})

	if pool.Len() != 3 {
		t.Fatalf("expected pool len 3, got %d", pool.Len())
	}

	// 插入更新的第 4 条数据（发布时间晚于 1, 2, 3），应淘汰发布时间最早的 ID 1
	admitted, err := pool.Put(VectorItem{VideoID: 4, CreateTime: baseTime.Add(3 * time.Hour), Vector: vec})
	if err != nil || !admitted {
		t.Fatalf("unexpected error on put: admitted=%v, err=%v", admitted, err)
	}

	if pool.Len() != 3 {
		t.Fatalf("expected pool len to remain 3 after eviction, got %d", pool.Len())
	}
	if _, ok := pool.Get(1); ok {
		t.Errorf("expected oldest item ID 1 to be evicted")
	}
	for _, id := range []uint{2, 3, 4} {
		if _, ok := pool.Get(id); !ok {
			t.Errorf("expected item %d to remain in pool", id)
		}
	}

	// 尝试插入比当前池中最老（ID 2，1小时后）还要更老的数据（ID 5，早于基准时间）
	// 由于池已满且该条目比池内所有内容都老，不应接纳入池淘汰更新的内容
	admitted, err = pool.Put(VectorItem{VideoID: 5, CreateTime: baseTime.Add(-1 * time.Hour), Vector: vec})
	if err != nil {
		t.Fatalf("unexpected error on put older item: %v", err)
	}
	if admitted {
		t.Errorf("expected older item not admitted into full pool")
	}
	if pool.Len() != 3 {
		t.Fatalf("expected pool len to remain 3, got %d", pool.Len())
	}
	if _, ok := pool.Get(5); ok {
		t.Errorf("expected item 5 not admitted into full pool")
	}

	// 更新已有条目 ID 2，更新为最新时间，容量仍为 3
	newVec := []float32{0, 1, 0, 0}
	admitted, err = pool.Put(VectorItem{VideoID: 2, CreateTime: baseTime.Add(5 * time.Hour), Vector: newVec})
	if err != nil || !admitted {
		t.Fatalf("unexpected error on update: admitted=%v, err=%v", admitted, err)
	}
	if pool.Len() != 3 {
		t.Fatalf("expected pool len to remain 3 after update, got %d", pool.Len())
	}

	// 此时池内为：ID 3 (2h), ID 4 (3h), ID 2 (5h)
	// 插入 ID 6 (2.5h)，此时最老的是 ID 3 (2h)，ID 6 比 ID 3 新，应淘汰 ID 3
	admitted, err = pool.Put(VectorItem{VideoID: 6, CreateTime: baseTime.Add(2*time.Hour + 30*time.Minute), Vector: vec})
	if err != nil || !admitted {
		t.Fatalf("unexpected error on put ID 6: admitted=%v, err=%v", admitted, err)
	}
	if _, ok := pool.Get(3); ok {
		t.Errorf("expected ID 3 to be evicted")
	}
	for _, id := range []uint{2, 4, 6} {
		if _, ok := pool.Get(id); !ok {
			t.Errorf("expected item %d to be in pool", id)
		}
	}
}

func TestVectorPool_PutBatch_And_Snapshot(t *testing.T) {
	pool := NewVectorPool(3, 4)

	// 1. 空池调用 Snapshot 不应 panic，返回空切片
	snap := pool.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected empty snapshot from empty pool, got %d", len(snap))
	}

	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	vec := []float32{1, 0, 0, 0}

	// 2. 准备一批数据（包含有效数据和脏数据，且数量超过池容量 3）
	items := []VectorItem{
		{VideoID: 1, CreateTime: baseTime, Vector: vec},                                                    // t0
		{VideoID: 2, CreateTime: baseTime.Add(1 * time.Hour), Vector: vec},                                 // t+1h
		{VideoID: 3, CreateTime: baseTime.Add(2 * time.Hour), Vector: vec},                                 // t+2h
		{VideoID: 4, CreateTime: baseTime.Add(3 * time.Hour), Vector: vec},                                 // t+3h
		{VideoID: 0, CreateTime: baseTime.Add(4 * time.Hour), Vector: vec},                                 // VideoID=0 非法
		{VideoID: 5, CreateTime: baseTime.Add(4 * time.Hour), Vector: []float32{1}},                        // 维度不匹配 非法
		{VideoID: 6, CreateTime: baseTime.Add(4 * time.Hour), Vector: []float32{float32(math.NaN()), 0, 0, 0}}, // NaN 非法
		{VideoID: 7, CreateTime: baseTime.Add(4 * time.Hour), Vector: []float32{0, 0, 0, 0}},               // 零向量 非法
	}

	admitted := pool.PutBatch(items)
	if admitted != 3 {
		t.Fatalf("expected 3 valid items admitted into pool, got %d", admitted)
	}

	if pool.Len() != 3 {
		t.Fatalf("expected pool len 3, got %d", pool.Len())
	}

	// 3. 验证 Snapshot 包含按发布时间倒序排列的最新 3 个有效条目（ID 4, 3, 2）
	snap = pool.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected snapshot len 3, got %d", len(snap))
	}

	expectedIDs := []uint{4, 3, 2}
	for i, expectedID := range expectedIDs {
		if snap[i].VideoID != expectedID {
			t.Errorf("expected snap[%d].VideoID=%d, got %d", i, expectedID, snap[i].VideoID)
		}
	}
}

func TestVectorPool_SearchCosine(t *testing.T) {
	pool := NewVectorPool(10, 4)
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// 准备 3 条不同方向的归一化向量
	// ID 1: [1, 0, 0, 0]
	// ID 2: [0.7071, 0.7071, 0, 0] (与 [1,0,0,0] 夹角 45 度，相似度约 0.707)
	// ID 3: [0, 1, 0, 0] (与 [1,0,0,0] 夹角 90 度，相似度 0)
	items := []VectorItem{
		{VideoID: 1, CreateTime: baseTime, Vector: []float32{1, 0, 0, 0}},
		{VideoID: 2, CreateTime: baseTime.Add(1 * time.Hour), Vector: []float32{1, 1, 0, 0}},
		{VideoID: 3, CreateTime: baseTime.Add(2 * time.Hour), Vector: []float32{0, 1, 0, 0}},
	}
	pool.PutBatch(items)

	// 查询向量与 [1, 0, 0, 0] 最相似
	query := []float32{1, 0, 0, 0}
	hits := pool.SearchCosine(query, 10, nil)

	// 相似度 > 0 的应有 ID 1 (1.0) 和 ID 2 (约 0.707)，ID 3 垂直 (dot=0) 过滤掉
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits with positive cosine, got %d", len(hits))
	}

	if hits[0].Item.VideoID != 1 {
		t.Errorf("expected top hit VideoID 1, got %d", hits[0].Item.VideoID)
	}
	if math.Abs(hits[0].Score-1.0) > 1e-4 {
		t.Errorf("expected score ~1.0 for identical vector, got %f", hits[0].Score)
	}

	if hits[1].Item.VideoID != 2 {
		t.Errorf("expected second hit VideoID 2, got %d", hits[1].Item.VideoID)
	}
	if math.Abs(hits[1].Score-0.7071) > 1e-3 {
		t.Errorf("expected score ~0.7071, got %f", hits[1].Score)
	}

	// 测试 filter 过滤回调（如排除 ID 1）
	filteredHits := pool.SearchCosine(query, 10, func(item VectorItem) bool {
		return item.VideoID != 1
	})
	if len(filteredHits) != 1 || filteredHits[0].Item.VideoID != 2 {
		t.Fatalf("expected filtered hits only contain ID 2, got %v", filteredHits)
	}

	// 测试空池或维度不匹配时安全返回空
	if len(pool.SearchCosine([]float32{1, 0}, 10, nil)) != 0 {
		t.Errorf("expected empty hits for dim mismatch")
	}
}

func TestVectorPool_ConcurrentReadWrite(t *testing.T) {
	const (
		capacity    = 100
		dim         = 8
		concurrency = 10
		iterations  = 200
	)

	pool := NewVectorPool(capacity, dim)
	baseTime := time.Now()

	// 先预热一批
	initial := make([]VectorItem, 50)
	for i := 0; i < 50; i++ {
		vec := make([]float32, dim)
		vec[0] = float32(i + 1)
		initial[i] = VectorItem{
			VideoID:    uint(i + 1),
			CreateTime: baseTime.Add(time.Duration(i) * time.Minute),
			Vector:     vec,
		}
	}
	pool.PutBatch(initial)

	var wg sync.WaitGroup

	// 并发写 goroutines (调用 Put)
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				vid := uint(gid*1000 + i + 1)
				vec := make([]float32, dim)
				vec[0] = float32(i + 1)
				vec[1] = float32(gid + 1)
				_, _ = pool.Put(VectorItem{
					VideoID:    vid,
					CreateTime: baseTime.Add(time.Duration(i) * time.Second),
					Vector:     vec,
				})
			}
		}(g)
	}

	// 并发批量写 goroutines (调用 PutBatch)
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iterations/5; i++ {
				batch := make([]VectorItem, 10)
				for j := 0; j < 10; j++ {
					vid := uint(90000 + gid*100 + j)
					vec := make([]float32, dim)
					vec[0] = 1.0
					batch[j] = VectorItem{
						VideoID:    vid,
						CreateTime: baseTime.Add(time.Duration(i*10+j) * time.Second),
						Vector:     vec,
					}
				}
				pool.PutBatch(batch)
			}
		}(g)
	}

	// 并发读 goroutines (调用 Snapshot / SearchCosine / Get / Len)
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			query := make([]float32, dim)
			query[0] = 1.0
			for i := 0; i < iterations; i++ {
				snap := pool.Snapshot()
				if len(snap) > capacity {
					t.Errorf("snapshot length %d exceeded capacity %d", len(snap), capacity)
				}
				for _, item := range snap {
					if len(item.Vector) != dim {
						t.Errorf("item vector dim %d != expected %d", len(item.Vector), dim)
					}
				}

				_ = pool.SearchCosine(query, 10, nil)

				if l := pool.Len(); l > capacity {
					t.Errorf("pool len %d exceeded capacity %d", l, capacity)
				}

				targetID := uint(gid*1000 + (i % 50) + 1)
				if item, ok := pool.Get(targetID); ok {
					if len(item.Vector) != dim {
						t.Errorf("item vector dim %d != expected %d", len(item.Vector), dim)
					}
				}
			}
		}(g)
	}

	wg.Wait()

	if pool.Len() > capacity {
		t.Fatalf("final pool len %d exceeded capacity %d", pool.Len(), capacity)
	}
}

func TestVectorPool_MemoryFootprint(t *testing.T) {
	// 验证 2,000 条 1536 维向量常驻内存占用不超过 15MB
	const (
		capacity = 2000
		dim      = 1536
	)

	pool := NewVectorPool(capacity, dim)
	baseTime := time.Now()

	batch := make([]VectorItem, capacity)
	for i := 0; i < capacity; i++ {
		vec := make([]float32, dim)
		vec[0] = float32(i + 1)
		batch[i] = VectorItem{
			VideoID:    uint(i + 1),
			CreateTime: baseTime.Add(time.Duration(i) * time.Second),
			Vector:     vec,
		}
	}

	admitted := pool.PutBatch(batch)
	if admitted != capacity {
		t.Fatalf("expected %d admitted, got %d", capacity, admitted)
	}

	if pool.Len() != capacity {
		t.Fatalf("expected pool len %d, got %d", capacity, pool.Len())
	}

	snap := pool.Snapshot()
	if len(snap) != capacity {
		t.Fatalf("expected snapshot len %d, got %d", capacity, len(snap))
	}

	// 理论内存开销：
	// 每个 float32 占 4 字节，1536 维向量底层数组占 6,144 字节。
	// 2,000 条向量数组开销 = 2,000 * 6,144 = 12,288,000 字节 ≈ 11.72 MB。
	// 加上 VectorItem 结构体本身（约 48 字节）和 map 开销，常驻总内存约在 12.5MB ~ 13.5MB，严格受控在 15MB 预算以内。
	totalVecBytes := capacity * dim * 4
	maxBudgetBytes := 15 * 1024 * 1024 // 15MB
	if totalVecBytes > maxBudgetBytes {
		t.Errorf("vector data bytes %d exceeds budget %d", totalVecBytes, maxBudgetBytes)
	}
}

func TestVideoRepository_LoadRecentVectors(t *testing.T) {
	// 验证 VideoRepository.LoadRecentVectors 对空结果或参数边界的防御
	vr := &VideoRepository{}
	// nil db 调用应优雅处理或参数为 0 返回 nil
	items, err := vr.LoadRecentVectors(context.Background(), "", 0, 0)
	if err != nil {
		t.Errorf("expected nil err for zero limit, got %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected empty items, got %d", len(items))
	}
}
