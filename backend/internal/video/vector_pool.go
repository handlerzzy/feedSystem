package video

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultVectorPoolCapacity 是内存向量池的默认容量上限（常驻 2,000 条活跃视频）。
// 2,000 条 1536 维 float32 向量在内存中约占 12MB，远低于 15MB 预算。
const DefaultVectorPoolCapacity = 2000

// DefaultVectorDim 是默认向量维度（FeedFlow 默认使用 1536 维 Embedding）。
const DefaultVectorDim = 1536

// VectorItem 是常驻内存向量池的一条视频向量单元。
type VectorItem struct {
	VideoID    uint
	CreateTime time.Time
	Vector     []float32
}

// ScoredVectorItem 带有余弦相似度分数的向量检索命中项。
type ScoredVectorItem struct {
	Item  VectorItem
	Score float64
}

// VectorPool 是常驻在 Go 服务堆内存中的并发安全热点视频向量集合。
// 为读链路提供微秒级的零 IO、零锁竞争余弦候选数据源。
type VectorPool struct {
	capacity int
	dim      int

	mu       sync.RWMutex
	items    map[uint]VectorItem
	snapshot atomic.Pointer[[]VectorItem]
}

// NewVectorPool 创建一个指定容量与维度的内存向量池。
func NewVectorPool(capacity, dim int) *VectorPool {
	if capacity <= 0 {
		capacity = DefaultVectorPoolCapacity
	}
	if dim <= 0 {
		dim = DefaultVectorDim
	}
	p := &VectorPool{
		capacity: capacity,
		dim:      dim,
		items:    make(map[uint]VectorItem, capacity),
	}
	empty := make([]VectorItem, 0)
	p.snapshot.Store(&empty)
	return p
}

// Capacity 返回向量池的最大容量。
func (p *VectorPool) Capacity() int {
	return p.capacity
}

// Dim 返回向量池配置的期望向量维度。
func (p *VectorPool) Dim() int {
	return p.dim
}

// Len 返回当前向量池内的有效向量条数。
func (p *VectorPool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.items)
}

// Get 根据视频 ID 获取单条向量项副本。
func (p *VectorPool) Get(videoID uint) (VectorItem, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	item, ok := p.items[videoID]
	if !ok {
		return VectorItem{}, false
	}
	copiedVec := make([]float32, len(item.Vector))
	copy(copiedVec, item.Vector)
	item.Vector = copiedVec
	return item, true
}

// Snapshot 获取当前池内候选集的只读切片快照。
// 基于 atomic.Pointer 实现零锁竞争、零内存分配读取（耗时 <10ns），适合 1000+ QPS 读链路。
// 注意：返回的快照切片为只读视图，调用方禁止就地修改切片内容。
func (p *VectorPool) Snapshot() []VectorItem {
	ptr := p.snapshot.Load()
	if ptr == nil || len(*ptr) == 0 {
		return nil
	}
	return *ptr
}

// normalizeAndValidate 校验向量合法性并返回 L2 归一化后的独立切片副本。
func (p *VectorPool) normalizeAndValidate(videoID uint, vec []float32) ([]float32, error) {
	if videoID == 0 {
		return nil, errors.New("video: vector pool 不允许写入 VideoID 为 0 的项")
	}
	if len(vec) == 0 {
		return nil, errors.New("video: vector pool 不允许写入空向量")
	}
	if len(vec) != p.dim {
		return nil, fmt.Errorf("video: 向量维度 %d 与池配置维度 %d 不匹配", len(vec), p.dim)
	}

	var sum float64
	for i, v := range vec {
		val := float64(v)
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil, fmt.Errorf("video: 向量第 %d 维不是有限数值 (NaN/Inf)", i)
		}
		sum += val * val
	}

	if sum <= 0 {
		return nil, errors.New("video: 零向量不携带有效语义信息，拒绝入池")
	}

	inv := 1 / math.Sqrt(sum)
	out := make([]float32, len(vec))
	for i, v := range vec {
		out[i] = float32(float64(v) * inv)
	}
	return out, nil
}

// rebuildSnapshotFromItemsLocked 重构只读快照切片（调用方必须持有 p.mu 写锁）。
func (p *VectorPool) rebuildSnapshotFromItemsLocked() {
	snap := make([]VectorItem, 0, len(p.items))
	for _, it := range p.items {
		snap = append(snap, it)
	}
	sortVectorItems(snap)
	p.snapshot.Store(&snap)
}

// sortVectorItems 统一按发布时间倒序、同时间按 VideoID 倒序排序。
func sortVectorItems(items []VectorItem) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].CreateTime.Equal(items[j].CreateTime) {
			return items[i].CreateTime.After(items[j].CreateTime)
		}
		return items[i].VideoID > items[j].VideoID
	})
}

// Put 校验并将一条向量写入池中。
// 返回 (admitted, error)：若数据非法返回 error；若因时效落后于满池最旧视频未被接纳返回 (false, nil)；成功入池返回 (true, nil)。
func (p *VectorPool) Put(item VectorItem) (bool, error) {
	normVec, err := p.normalizeAndValidate(item.VideoID, item.Vector)
	if err != nil {
		return false, err
	}

	storedItem := VectorItem{
		VideoID:    item.VideoID,
		CreateTime: item.CreateTime,
		Vector:     normVec,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 1. 若条目已在池中，就地更新，容量不变
	if _, exists := p.items[item.VideoID]; exists {
		p.items[item.VideoID] = storedItem
		p.rebuildSnapshotFromItemsLocked()
		return true, nil
	}

	// 2. 若池容量已满，检查是否需要淘汰最老的视频
	if len(p.items) >= p.capacity {
		var oldestID uint
		var oldestTime time.Time
		first := true

		for id, it := range p.items {
			if first || it.CreateTime.Before(oldestTime) || (it.CreateTime.Equal(oldestTime) && it.VideoID < oldestID) {
				oldestID = id
				oldestTime = it.CreateTime
				first = false
			}
		}

		// 若新插入条目比池内最老的条目还要老（或相同时间但 VideoID 更小），则不接纳入池
		if !first {
			if storedItem.CreateTime.Before(oldestTime) || (storedItem.CreateTime.Equal(oldestTime) && storedItem.VideoID <= oldestID) {
				return false, nil
			}
			delete(p.items, oldestID)
		}
	}

	p.items[item.VideoID] = storedItem
	p.rebuildSnapshotFromItemsLocked()
	return true, nil
}

// PutBatch 批量预热或增量填充向量池。
// 过滤非法数据，保留时间最新的最多 capacity 条，并原子更新快照。返回成功入池的条目数。
func (p *VectorPool) PutBatch(items []VectorItem) int {
	if len(items) == 0 {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 1. 过滤并规范化有效条目
	validMap := make(map[uint]VectorItem, len(items))
	for _, it := range items {
		normVec, err := p.normalizeAndValidate(it.VideoID, it.Vector)
		if err != nil {
			continue
		}
		validMap[it.VideoID] = VectorItem{
			VideoID:    it.VideoID,
			CreateTime: it.CreateTime,
			Vector:     normVec,
		}
	}

	if len(validMap) == 0 {
		return 0
	}

	// 2. 合并当前池内条目与新条目
	all := make([]VectorItem, 0, len(p.items)+len(validMap))
	for id, it := range p.items {
		if _, updated := validMap[id]; !updated {
			all = append(all, it)
		}
	}
	for _, it := range validMap {
		all = append(all, it)
	}

	// 3. 按发布时间倒序排序
	sortVectorItems(all)

	// 4. 截取最多 capacity 条
	if len(all) > p.capacity {
		all = all[:p.capacity]
	}

	// 5. 更新 items map
	clear(p.items)
	admittedCount := 0
	for _, it := range all {
		p.items[it.VideoID] = it
		if _, ok := validMap[it.VideoID]; ok {
			admittedCount++
		}
	}

	// 6. 原子重建只读快照（复用已排好序的 all 切片副本，避免二次排序）
	snap := make([]VectorItem, len(all))
	copy(snap, all)
	p.snapshot.Store(&snap)

	return admittedCount
}

// SearchCosine 在内存向量池中对输入向量执行余弦相似度计算，排序并返回 Top-K 结果。
// 支持通过 filter 回调前置排除曝光、冷启动或作者自身视频。计算完全常驻内存，耗时 <2ms。
func (p *VectorPool) SearchCosine(query []float32, topK int, filter func(item VectorItem) bool) []ScoredVectorItem {
	if topK <= 0 || len(query) != p.dim {
		return nil
	}

	candidates := p.Snapshot()
	if len(candidates) == 0 {
		return nil
	}

	// 确保 query 处于归一化状态（若未归一化就地归一化）
	normQuery := query
	var sum float64
	for _, v := range query {
		sum += float64(v) * float64(v)
	}
	if sum <= 0 {
		return nil
	}
	if math.Abs(sum-1.0) > 1e-4 {
		inv := 1 / math.Sqrt(sum)
		normQuery = make([]float32, len(query))
		for i, v := range query {
			normQuery[i] = float32(float64(v) * inv)
		}
	}

	scored := make([]ScoredVectorItem, 0, len(candidates))
	for _, cand := range candidates {
		if filter != nil && !filter(cand) {
			continue
		}

		// 双方均已 L2 归一化，点积即余弦相似度
		var dot float64
		for i := 0; i < p.dim; i++ {
			dot += float64(normQuery[i]) * float64(cand.Vector[i])
		}

		// 仅保留正相关分数 (score > 0)
		if dot > 0 {
			scored = append(scored, ScoredVectorItem{
				Item:  cand,
				Score: dot,
			})
		}
	}

	// 按分数降序，同分按发布时间降序、同时间按 VideoID 降序
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].Score != scored[j].Score {
			return scored[i].Score > scored[j].Score
		}
		if !scored[i].Item.CreateTime.Equal(scored[j].Item.CreateTime) {
			return scored[i].Item.CreateTime.After(scored[j].Item.CreateTime)
		}
		return scored[i].Item.VideoID > scored[j].Item.VideoID
	})

	if len(scored) > topK {
		scored = scored[:topK]
	}
	return scored
}
