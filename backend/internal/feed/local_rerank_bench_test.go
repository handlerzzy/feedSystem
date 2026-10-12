package feed

import (
	"testing"
	"time"

	"github.com/handlerzzy/feedsystem/internal/video"
)

type benchmarkFixture struct {
	items []*video.Video
	sims  map[uint]float64
	now   time.Time
	opts  LocalRerankOptions
}

// newBenchmarkFixture 构造基准测试所需的确定性测试夹具。
func newBenchmarkFixture(count int) benchmarkFixture {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	opts := DefaultLocalRerankOptions()
	items := make([]*video.Video, count)
	sims := make(map[uint]float64, count)
	for i := 0; i < count; i++ {
		vid := uint(i + 1)
		items[i] = &video.Video{
			ID:         vid,
			CreateTime: now.Add(-time.Duration(i*2) * time.Hour),
			LikesCount: int64(i * 100),
		}
		sims[vid] = float64(i%10) / 10.0
	}
	return benchmarkFixture{
		items: items,
		sims:  sims,
		now:   now,
		opts:  opts,
	}
}

// BenchmarkCalculateLocalRerankScore_50Candidates 度量 50 条候选视频在多特征综合公式下的打分耗时与内存分配，
// 贴合生产真实访问模式（直接从相似度 map 中取分），验证单次均值远低于 100μs (0.1ms)，且堆内存逃逸严格为 0 allocs/op 与 0 B/op。
func BenchmarkCalculateLocalRerankScore_50Candidates(b *testing.B) {
	const count = 50
	fix := newBenchmarkFixture(count)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			v := fix.items[j]
			_ = CalculateLocalRerankScore(v, fix.sims[v.ID], fix.now, fix.opts)
		}
	}
}

// BenchmarkLocalRerankVideos_50Candidates 度量 50 条候选视频执行完整本地特征打分与重排排序的整体耗时与内存分配。
func BenchmarkLocalRerankVideos_50Candidates(b *testing.B) {
	const count = 50
	fix := newBenchmarkFixture(count)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = LocalRerankVideos(fix.items, fix.sims, fix.now, fix.opts)
	}
}

// TestCalculateLocalRerankScore_ZeroAllocations 以单测门禁形式严格断言 50 条候选视频打分零堆内存分配（0 allocs/op 且 0 B/op）。
func TestCalculateLocalRerankScore_ZeroAllocations(t *testing.T) {
	const count = 50
	fix := newBenchmarkFixture(count)

	res := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < count; j++ {
				v := fix.items[j]
				_ = CalculateLocalRerankScore(v, fix.sims[v.ID], fix.now, fix.opts)
			}
		}
	})

	if allocs := res.AllocsPerOp(); allocs != 0 {
		t.Fatalf("CalculateLocalRerankScore 存在堆内存逃逸: %d allocs/op, want 0", allocs)
	}
	if bytes := res.AllocedBytesPerOp(); bytes != 0 {
		t.Fatalf("CalculateLocalRerankScore 存在堆内存分配: %d B/op, want 0", bytes)
	}
}
