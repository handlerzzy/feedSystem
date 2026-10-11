package video

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/handlerzzy/feedsystem/internal/ai"
)

// 本文件用手写假实现覆盖回填逻辑里最容易错、又最难在真机上构造的几条：
//
//   - "内容没变就不重算"（否则每轮都重算全库，模型成本随轮次线性上涨）；
//   - "内容变了必须重算"（否则改了标题的视频会一直用旧向量被召回，
//     表现为推荐理由与内容对不上）；
//   - "结果条数/维度不符时整批丢弃"（否则文本与向量永久错配）；
//   - "零向量不入库"（否则它永远占据召回池却永远排不上来）。

type fakeBackfillStore struct {
	targets          []EmbeddingTarget
	upserted         [][]VideoEmbedding
	upsertErr        error
	recordedAfterIDs []uint
}

func (s *fakeBackfillStore) ListEmbeddingTargets(_ context.Context, afterID uint, _ string, _ int, limit int) ([]EmbeddingTarget, error) {
	s.recordedAfterIDs = append(s.recordedAfterIDs, afterID)
	var out []EmbeddingTarget
	for _, t := range s.targets {
		if t.VideoID > afterID {
			out = append(out, t)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *fakeBackfillStore) GetEmbeddingTarget(_ context.Context, videoID uint, _ string, _ int) (*EmbeddingTarget, error) {
	for _, t := range s.targets {
		if t.VideoID == videoID {
			targetCopy := t
			return &targetCopy, nil
		}
	}
	return nil, nil
}

func (s *fakeBackfillStore) UpsertEmbeddings(_ context.Context, rows []VideoEmbedding) error {
	if s.upsertErr != nil {
		return s.upsertErr
	}
	cp := make([]VideoEmbedding, len(rows))
	copy(cp, rows)
	s.upserted = append(s.upserted, cp)
	return nil
}

func (s *fakeBackfillStore) allRows() []VideoEmbedding {
	var out []VideoEmbedding
	for _, batch := range s.upserted {
		out = append(out, batch...)
	}
	return out
}

// fakeEmbedClient 按"输入文本"返回确定性向量，并允许注入失败与坏结果。
type fakeEmbedClient struct {
	calls    int
	inputs   [][]string
	failWith error
	// mutate 允许在返回前篡改结果，用来构造"契约不满足"的场景。
	mutate func(req ai.EmbeddingRequest, res ai.EmbeddingResult) ai.EmbeddingResult
}

func (c *fakeEmbedClient) Embed(_ context.Context, req ai.EmbeddingRequest) (ai.EmbeddingResult, error) {
	c.calls++
	c.inputs = append(c.inputs, append([]string(nil), req.Inputs...))
	if c.failWith != nil {
		return ai.EmbeddingResult{}, c.failWith
	}
	vectors := make([][]float32, len(req.Inputs))
	for i, text := range req.Inputs {
		vectors[i] = deterministicVector(text, 4)
	}
	res := ai.EmbeddingResult{
		Vectors:    vectors,
		Model:      req.Model,
		Dim:        4,
		Normalized: req.Normalize,
	}
	if c.mutate != nil {
		res = c.mutate(req, res)
	}
	return res, nil
}

func (c *fakeEmbedClient) Health(context.Context) (ai.EmbeddingHealth, error) {
	return ai.EmbeddingHealth{Serving: true}, nil
}

func (c *fakeEmbedClient) Close() error { return nil }

// deterministicVector 返回一个非零、且随文本变化的 4 维向量。
func deterministicVector(text string, dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(len(text)%7+1) / float32(i+1)
	}
	vec[0] += float32(len(text) % 3)
	return vec
}

func testBackfillOptions() BackfillOptions {
	opts := DefaultBackfillOptions()
	opts.Model = "test-model"
	opts.Dim = 4
	opts.EmbedBatch = 2
	opts.SkipZero = true
	return opts
}

func TestBackfillSkipsUnchangedAndEmbedsStale(t *testing.T) {
	store := &fakeBackfillStore{targets: []EmbeddingTarget{
		// 已有向量且指纹一致 -> 不该重算。
		{VideoID: 1, Title: "已向量化", Description: "内容没变", CurrentHash: "h1", StoredHash: "h1"},
		// 从来没有向量 -> 要算。
		{VideoID: 2, Title: "新视频", Description: "还没向量", CurrentHash: "h2"},
		// 内容变了 -> 要重算。
		{VideoID: 3, Title: "改了标题", Description: "旧向量已过期", CurrentHash: "h3-new", StoredHash: "h3-old"},
	}}
	client := &fakeEmbedClient{}
	b := NewEmbeddingBackfiller(store, client, testBackfillOptions())

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("BackfillOnce 返回错误: %v", err)
	}
	if stats.Scanned != 3 {
		t.Errorf("Scanned = %d, want 3", stats.Scanned)
	}
	if stats.Stale != 2 {
		t.Errorf("Stale = %d, want 2（只有 2、3 需要重算）", stats.Stale)
	}
	if stats.Embedded != 2 {
		t.Errorf("Embedded = %d, want 2", stats.Embedded)
	}

	rows := store.allRows()
	if len(rows) != 2 {
		t.Fatalf("入库行数 = %d, want 2", len(rows))
	}
	seen := map[uint]bool{}
	for _, r := range rows {
		seen[r.VideoID] = true
		if r.ContentHash == "" {
			t.Errorf("vid=%d 的 content_hash 为空：没有它就检测不出内容变更", r.VideoID)
		}
		if len(r.Vector) != 16 {
			t.Errorf("vid=%d 的向量字节数 = %d, want 16", r.VideoID, len(r.Vector))
		}
		if !r.Normalized {
			t.Errorf("vid=%d 的 normalized 为 false，而请求要求了归一化", r.VideoID)
		}
	}
	if seen[1] {
		t.Error("vid=1 内容没变却被重算：每轮都会重算全库")
	}
	if !seen[2] || !seen[3] {
		t.Errorf("漏算了需要重算的视频: %v", seen)
	}
	// 第 3 条必须带上**新的**指纹，否则下一轮会再次判定为过期（无限重算）。
	for _, r := range rows {
		if r.VideoID == 3 && r.ContentHash != "h3-new" {
			t.Errorf("vid=3 写入的指纹 = %q, want h3-new", r.ContentHash)
		}
	}
}

func TestBackfillBatchesRespectEmbedBatch(t *testing.T) {
	store := &fakeBackfillStore{}
	for id := uint(1); id <= 5; id++ {
		store.targets = append(store.targets, EmbeddingTarget{
			VideoID: id, Title: "标题", CurrentHash: "h",
		})
	}
	client := &fakeEmbedClient{}
	opts := testBackfillOptions()
	opts.EmbedBatch = 2
	b := NewEmbeddingBackfiller(store, client, opts)

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("BackfillOnce 返回错误: %v", err)
	}
	if stats.Embedded != 5 {
		t.Fatalf("Embedded = %d, want 5", stats.Embedded)
	}
	if client.calls != 3 {
		t.Errorf("调用次数 = %d, want 3（5 条按每批 2 条切分）", client.calls)
	}
	for i, in := range client.inputs {
		if len(in) > 2 {
			t.Errorf("第 %d 批 %d 条，超过 EmbedBatch=2", i, len(in))
		}
	}
}

func TestBackfillEmptyTextIsSkippedNotFailed(t *testing.T) {
	store := &fakeBackfillStore{targets: []EmbeddingTarget{
		{VideoID: 1, Title: "   ", Description: "  ", CurrentHash: "h1"},
		{VideoID: 2, Title: "有文本", CurrentHash: "h2"},
	}}
	client := &fakeEmbedClient{}
	b := NewEmbeddingBackfiller(store, client, testBackfillOptions())

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("BackfillOnce 返回错误: %v", err)
	}
	if stats.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1", stats.Skipped)
	}
	if stats.Failed != 0 {
		t.Errorf("Failed = %d, want 0（没有文本不是故障）", stats.Failed)
	}
	if stats.Embedded != 1 {
		t.Errorf("Embedded = %d, want 1", stats.Embedded)
	}
}

func TestBackfillZeroVectorIsNotStored(t *testing.T) {
	store := &fakeBackfillStore{targets: []EmbeddingTarget{
		{VideoID: 1, Title: "零向量", CurrentHash: "h1"},
	}}
	client := &fakeEmbedClient{mutate: func(_ ai.EmbeddingRequest, res ai.EmbeddingResult) ai.EmbeddingResult {
		res.Vectors[0] = []float32{0, 0, 0, 0}
		return res
	}}
	b := NewEmbeddingBackfiller(store, client, testBackfillOptions())

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("BackfillOnce 返回错误: %v", err)
	}
	if stats.Embedded != 0 || stats.Skipped != 1 {
		t.Errorf("Embedded=%d Skipped=%d, want 0/1（零向量必须被丢弃）", stats.Embedded, stats.Skipped)
	}
	if len(store.allRows()) != 0 {
		t.Error("零向量被写进了库：它与任何向量的余弦都是 0")
	}
}

func TestBackfillRejectsContractViolation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(req ai.EmbeddingRequest, res ai.EmbeddingResult) ai.EmbeddingResult
	}{
		{
			name: "条数不一致",
			mutate: func(_ ai.EmbeddingRequest, res ai.EmbeddingResult) ai.EmbeddingResult {
				res.Vectors = res.Vectors[:len(res.Vectors)-1]
				return res
			},
		},
		{
			name: "维度不符",
			mutate: func(_ ai.EmbeddingRequest, res ai.EmbeddingResult) ai.EmbeddingResult {
				res.Dim = 3
				res.Vectors[0] = []float32{1, 2, 3}
				return res
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeBackfillStore{targets: []EmbeddingTarget{
				{VideoID: 1, Title: "甲", CurrentHash: "h1"},
				{VideoID: 2, Title: "乙", CurrentHash: "h2"},
			}}
			client := &fakeEmbedClient{mutate: tc.mutate}
			b := NewEmbeddingBackfiller(store, client, testBackfillOptions())

			stats, err := b.BackfillOnce(context.Background())
			if err != nil {
				t.Fatalf("BackfillOnce 返回错误: %v", err)
			}
			if stats.Embedded != 0 {
				t.Errorf("Embedded = %d, want 0（契约不满足时必须整批丢弃）", stats.Embedded)
			}
			if stats.Failed != 2 {
				t.Errorf("Failed = %d, want 2", stats.Failed)
			}
			if len(store.allRows()) != 0 {
				t.Error("契约不满足的整批结果被写进了库：文本与向量会永久错配")
			}
		})
	}
}

func TestBackfillEmbedFailureDoesNotStopOtherBatches(t *testing.T) {
	store := &fakeBackfillStore{}
	for id := uint(1); id <= 3; id++ {
		store.targets = append(store.targets, EmbeddingTarget{VideoID: id, Title: "标题", CurrentHash: "h"})
	}
	client := &fakeEmbedClient{failWith: ai.ErrUnavailable}
	opts := testBackfillOptions()
	opts.EmbedBatch = 2
	b := NewEmbeddingBackfiller(store, client, opts)

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("整批失败不该让 BackfillOnce 返回错误: %v", err)
	}
	if stats.Failed != 3 || stats.Embedded != 0 {
		t.Errorf("Failed=%d Embedded=%d, want 3/0", stats.Failed, stats.Embedded)
	}
	if stats.Pending != 3 {
		t.Errorf("Pending = %d, want 3（下一轮还要重试）", stats.Pending)
	}
}

func TestBackfillUpsertFailureIsCountedAsFailure(t *testing.T) {
	store := &fakeBackfillStore{
		targets:   []EmbeddingTarget{{VideoID: 1, Title: "甲", CurrentHash: "h1"}},
		upsertErr: errors.New("db down"),
	}
	client := &fakeEmbedClient{}
	b := NewEmbeddingBackfiller(store, client, testBackfillOptions())

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("BackfillOnce 返回错误: %v", err)
	}
	if stats.Embedded != 0 || stats.Failed != 1 {
		t.Errorf("Embedded=%d Failed=%d, want 0/1（写库失败不能被算成成功）", stats.Embedded, stats.Failed)
	}
}

func TestBackfillOptionsDefaultsAreSafe(t *testing.T) {
	opts := BackfillOptions{}.withDefaults()
	if opts.Model == "" || opts.Dim <= 0 {
		t.Fatalf("模型与维度必须有默认值: %+v", opts)
	}
	if opts.EmbedBatch > MaxEmbedBatch {
		t.Fatalf("EmbedBatch = %d，超过 proto 契约的 %d 条（sidecar 会拒绝整批）",
			opts.EmbedBatch, MaxEmbedBatch)
	}
	// 显式超过上限时必须被夹住，而不是原样送到 sidecar。
	bid := BackfillOptions{EmbedBatch: 1000}.withDefaults()
	if bid.EmbedBatch != MaxEmbedBatch {
		t.Fatalf("EmbedBatch = %d, want %d", bid.EmbedBatch, MaxEmbedBatch)
	}
	if !opts.NormalizeOr() {
		t.Error("默认必须要求归一化：未归一化的向量会让更长的文本系统性占优")
	}
	// 显式要求"不归一化"必须能被表达出来（这正是用 *bool 而不是 bool 的理由）。
	if (BackfillOptions{Normalize: boolPtr(false)}).NormalizeOr() {
		t.Error("显式的 Normalize=false 被缺省值覆盖了")
	}
}

func TestEmbeddingTextJoinsTitleAndDescription(t *testing.T) {
	// 拼法必须与 ContentHash 的输入一致（见 EmbeddingText 的注释）。
	cases := []struct{ title, desc, want string }{
		{"标题", "描述", "标题。描述"},
		{"标题", "", "标题"},
		{"", "描述", "描述"},
		{"  标题  ", "  描述  ", "标题。描述"},
	}
	for _, tc := range cases {
		if got := EmbeddingText(tc.title, tc.desc); got != tc.want {
			t.Errorf("EmbeddingText(%q,%q) = %q, want %q", tc.title, tc.desc, got, tc.want)
		}
	}
	if !strings.Contains(EmbeddingText("标题", "描述"), "标题") {
		t.Error("拼接结果必须包含标题")
	}
}

func TestBackfillCursorAdvancesAndRollsOver(t *testing.T) {
	// 构造 1200 条视频数据，测试突破 500 阈值时的分批推进与触达表尾复位
	store := &fakeBackfillStore{}
	for id := uint(1); id <= 1200; id++ {
		store.targets = append(store.targets, EmbeddingTarget{
			VideoID:     id,
			Title:       "视频",
			CurrentHash: "h",
			StoredHash:  "h", // 内容未变，无需真正调模型，专注测扫描与游标
		})
	}
	client := &fakeEmbedClient{}
	opts := testBackfillOptions()
	opts.BatchLimit = 500
	b := NewEmbeddingBackfiller(store, client, opts)

	// 第 1 轮：从 0 开始扫，拉取 1..500（500 条）
	stats1, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("第 1 轮 BackfillOnce 失败: %v", err)
	}
	if stats1.Scanned != 500 {
		t.Fatalf("第 1 轮 Scanned = %d, want 500", stats1.Scanned)
	}
	if b.Cursor() != 500 {
		t.Fatalf("第 1 轮游标 = %d, want 500", b.Cursor())
	}
	if stats1.Cursor != 500 {
		t.Fatalf("第 1 轮 stats.Cursor = %d, want 500", stats1.Cursor)
	}

	// 第 2 轮：游标从 500 推进，拉取 501..1000（500 条）
	stats2, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("第 2 轮 BackfillOnce 失败: %v", err)
	}
	if stats2.Scanned != 500 {
		t.Fatalf("第 2 轮 Scanned = %d, want 500", stats2.Scanned)
	}
	if b.Cursor() != 1000 {
		t.Fatalf("第 2 轮游标 = %d, want 1000", b.Cursor())
	}

	// 第 3 轮：游标从 1000 推进，拉取 1001..1200（200 条，不足 BatchLimit，说明已达表尾）
	stats3, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("第 3 轮 BackfillOnce 失败: %v", err)
	}
	if stats3.Scanned != 200 {
		t.Fatalf("第 3 轮 Scanned = %d, want 200", stats3.Scanned)
	}
	// 扫到末尾时自动复位到 0，用于下一轮全表巡检
	if b.Cursor() != 0 {
		t.Fatalf("扫到表尾后游标必须自动复位为 0，当前为 %d", b.Cursor())
	}

	// 第 4 轮：验证已复位，从 0 重新开始扫描巡检
	stats4, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("第 4 轮 BackfillOnce 失败: %v", err)
	}
	if stats4.Scanned != 500 {
		t.Fatalf("第 4 轮 Scanned = %d, want 500", stats4.Scanned)
	}
	if b.Cursor() != 500 {
		t.Fatalf("第 4 轮游标 = %d, want 500", b.Cursor())
	}

	// 验证向仓储层传递的 afterID 参数序列
	expectedAfterIDs := []uint{0, 500, 1000, 0}
	if len(store.recordedAfterIDs) != len(expectedAfterIDs) {
		t.Fatalf("recordedAfterIDs count = %d, want %d", len(store.recordedAfterIDs), len(expectedAfterIDs))
	}
	for i, wantID := range expectedAfterIDs {
		if store.recordedAfterIDs[i] != wantID {
			t.Errorf("第 %d 次查询 afterID = %d, want %d", i+1, store.recordedAfterIDs[i], wantID)
		}
	}
}

func TestBackfillContentHashMismatchTriggersRecomputeAndSelfHeal(t *testing.T) {
	pool := NewVectorPool(10, 4)
	store := &fakeBackfillStore{
		targets: []EmbeddingTarget{
			{VideoID: 10, Title: "改前旧标题", Description: "描述", CurrentHash: "hash-new", StoredHash: "hash-old"},
		},
	}
	client := &fakeEmbedClient{}
	b := NewEmbeddingBackfiller(store, client, testBackfillOptions()).WithVectorPool(pool)

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("BackfillOnce 失败: %v", err)
	}
	if stats.Stale != 1 || stats.Embedded != 1 {
		t.Fatalf("Stale=%d Embedded=%d, want 1/1", stats.Stale, stats.Embedded)
	}
	rows := store.allRows()
	if len(rows) != 1 {
		t.Fatalf("落库行数 = %d, want 1", len(rows))
	}
	if rows[0].ContentHash != "hash-new" {
		t.Fatalf("落库 ContentHash = %s, want hash-new", rows[0].ContentHash)
	}

	// 验证同步推入 VectorPool
	item, ok := pool.Get(10)
	if !ok {
		t.Fatal("变更重算后未能推入 VectorPool")
	}
	if item.VideoID != 10 || len(item.Vector) != 4 {
		t.Fatalf("VectorPool 中的项目不符合预期: %+v", item)
	}
}

func TestBackfillSyncsToVectorPool(t *testing.T) {
	pool := NewVectorPool(10, 4)
	store := &fakeBackfillStore{
		targets: []EmbeddingTarget{
			{VideoID: 1, Title: "视频1", CurrentHash: "h1"},
			{VideoID: 2, Title: "视频2", CurrentHash: "h2"},
		},
	}
	client := &fakeEmbedClient{}
	b := NewEmbeddingBackfiller(store, client, testBackfillOptions()).WithVectorPool(pool)

	stats, err := b.BackfillOnce(context.Background())
	if err != nil {
		t.Fatalf("BackfillOnce 失败: %v", err)
	}
	if stats.Embedded != 2 {
		t.Fatalf("Embedded = %d, want 2", stats.Embedded)
	}
	if pool.Len() != 2 {
		t.Fatalf("pool.Len = %d, want 2", pool.Len())
	}
	if _, ok := pool.Get(1); !ok {
		t.Error("vid=1 未同步至 VectorPool")
	}
	if _, ok := pool.Get(2); !ok {
		t.Error("vid=2 未同步至 VectorPool")
	}
}

func TestEmbedVideo_InstantSingleVideoAndContentHashCheck(t *testing.T) {
	pool := NewVectorPool(10, 4)
	store := &fakeBackfillStore{
		targets: []EmbeddingTarget{
			{VideoID: 101, Title: "新发布的视频", Description: "刚发布", CurrentHash: "h101", StoredHash: ""},
			{VideoID: 102, Title: "已存在的视频", Description: "未变动", CurrentHash: "h102", StoredHash: "h102"},
			{VideoID: 103, Title: "修改了标题的视频", Description: "更新了", CurrentHash: "h103-new", StoredHash: "h103-old"},
		},
	}
	client := &fakeEmbedClient{}
	b := NewEmbeddingBackfiller(store, client, testBackfillOptions()).WithVectorPool(pool)

	ctx := context.Background()

	// 1. 新发布视频（无 StoredHash）：触发计算、入库并同步入池
	if err := b.EmbedVideo(ctx, 101); err != nil {
		t.Fatalf("EmbedVideo(101) 失败: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("新发布视频应当调用模型，实际 calls=%d", client.calls)
	}
	if _, ok := pool.Get(101); !ok {
		t.Fatal("新视频未能推入 VectorPool")
	}

	// 2. 已存在且指纹一致的视频：跳过计算（不调用模型）
	if err := b.EmbedVideo(ctx, 102); err != nil {
		t.Fatalf("EmbedVideo(102) 失败: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("内容未变时不应调用模型，实际 calls=%d", client.calls)
	}

	// 3. 内容修改的视频（指纹不一致）：重新计算并覆盖入库与入池
	if err := b.EmbedVideo(ctx, 103); err != nil {
		t.Fatalf("EmbedVideo(103) 失败: %v", err)
	}
	if client.calls != 2 {
		t.Fatalf("内容更新视频应当调用模型重算，实际 calls=%d", client.calls)
	}
	if _, ok := pool.Get(103); !ok {
		t.Fatal("内容更新视频未能推入 VectorPool")
	}

	// 4. 不存在的视频：返回明确错误
	if err := b.EmbedVideo(ctx, 9999); err == nil {
		t.Fatal("不存在的视频应当返回错误")
	}
}

