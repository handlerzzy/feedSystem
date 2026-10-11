package video

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
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

	numWorkers := 1
	if len(candidates) >= 500 {
		numWorkers = 4
		if maxProcs := runtime.GOMAXPROCS(0); maxProcs < numWorkers {
			numWorkers = maxProcs
		}
		if numWorkers < 1 {
			numWorkers = 1
		}
	}

	if numWorkers == 1 {
		return searchCosineChunk(normQuery, candidates, topK, filter)
	}

	chunkSize := (len(candidates) + numWorkers - 1) / numWorkers
	workerHeaps := make([][]ScoredVectorItem, numWorkers)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		if start >= len(candidates) {
			break
		}
		end := start + chunkSize
		if end > len(candidates) {
			end = len(candidates)
		}
		wg.Add(1)
		go func(workerIdx int, chunk []VectorItem) {
			defer wg.Done()
			workerHeaps[workerIdx] = searchCosineChunk(normQuery, chunk, topK, filter)
		}(w, candidates[start:end])
	}
	wg.Wait()

	finalHeap := make([]ScoredVectorItem, 0, topK)
	for _, wh := range workerHeaps {
		for _, item := range wh {
			if len(finalHeap) < topK {
				finalHeap = append(finalHeap, item)
				siftUpScored(finalHeap, len(finalHeap)-1)
			} else if itemLess(&finalHeap[0], &item) {
				finalHeap[0] = item
				siftDownScored(finalHeap, 0, topK)
			}
		}
	}

	slices.SortFunc(finalHeap, func(a, b ScoredVectorItem) int {
		if a.Score != b.Score {
			if a.Score > b.Score {
				return -1
			}
			return 1
		}
		if !a.Item.CreateTime.Equal(b.Item.CreateTime) {
			if a.Item.CreateTime.After(b.Item.CreateTime) {
				return -1
			}
			return 1
		}
		if a.Item.VideoID > b.Item.VideoID {
			return -1
		} else if a.Item.VideoID < b.Item.VideoID {
			return 1
		}
		return 0
	})

	return finalHeap
}

func searchCosineChunk(normQuery []float32, candidates []VectorItem, topK int, filter func(item VectorItem) bool) []ScoredVectorItem {
	heap := make([]ScoredVectorItem, 0, topK)
	for _, cand := range candidates {
		if filter != nil && !filter(cand) {
			continue
		}

		dot := cosineDotProduct(normQuery, cand.Vector)
		if dot <= 0 {
			continue
		}

		item := ScoredVectorItem{
			Item:  cand,
			Score: dot,
		}

		if len(heap) < topK {
			heap = append(heap, item)
			siftUpScored(heap, len(heap)-1)
		} else if itemLess(&heap[0], &item) {
			heap[0] = item
			siftDownScored(heap, 0, topK)
		}
	}

	slices.SortFunc(heap, func(a, b ScoredVectorItem) int {
		if a.Score != b.Score {
			if a.Score > b.Score {
				return -1
			}
			return 1
		}
		if !a.Item.CreateTime.Equal(b.Item.CreateTime) {
			if a.Item.CreateTime.After(b.Item.CreateTime) {
				return -1
			}
			return 1
		}
		if a.Item.VideoID > b.Item.VideoID {
			return -1
		} else if a.Item.VideoID < b.Item.VideoID {
			return 1
		}
		return 0
	})

	return heap
}

// itemLess 用于小顶堆判定：a 是否劣于 b（分数更低，同分时间更早，同时间 ID 更小）。
func itemLess(a, b *ScoredVectorItem) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	if !a.Item.CreateTime.Equal(b.Item.CreateTime) {
		return a.Item.CreateTime.Before(b.Item.CreateTime)
	}
	return a.Item.VideoID < b.Item.VideoID
}

func siftUpScored(h []ScoredVectorItem, i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !itemLess(&h[i], &h[p]) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func siftDownScored(h []ScoredVectorItem, i, n int) {
	for {
		left := 2*i + 1
		if left >= n || left < 0 {
			break
		}
		smallest := left
		if right := left + 1; right < n && itemLess(&h[right], &h[left]) {
			smallest = right
		}
		if !itemLess(&h[smallest], &h[i]) {
			break
		}
		h[i], h[smallest] = h[smallest], h[i]
		i = smallest
	}
}

// cosineDotProduct 在两个已知长度相等的归一化向量之间计算点积。
// 通过 16 路累加器展开与 BCE（边界检查消除）优化流水线，使 2,000 候选检索控制在 <1ms 内。
func cosineDotProduct(a, b []float32) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var s0, s1, s2, s3, s4, s5, s6, s7 float32
	var s8, s9, s10, s11, s12, s13, s14, s15 float32
	i := 0
	for ; i+16 <= n; i += 16 {
		_ = a[i+15]
		_ = b[i+15]
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
		s4 += a[i+4] * b[i+4]
		s5 += a[i+5] * b[i+5]
		s6 += a[i+6] * b[i+6]
		s7 += a[i+7] * b[i+7]
		s8 += a[i+8] * b[i+8]
		s9 += a[i+9] * b[i+9]
		s10 += a[i+10] * b[i+10]
		s11 += a[i+11] * b[i+11]
		s12 += a[i+12] * b[i+12]
		s13 += a[i+13] * b[i+13]
		s14 += a[i+14] * b[i+14]
		s15 += a[i+15] * b[i+15]
	}
	s := (s0 + s1 + s2 + s3) + (s4 + s5 + s6 + s7) + (s8 + s9 + s10 + s11) + (s12 + s13 + s14 + s15)
	for ; i < n; i++ {
		s += a[i] * b[i]
	}
	return float64(s)
}
