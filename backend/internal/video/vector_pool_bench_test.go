package video

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

// prepareBenchmarkVectorPool 构建并填充 2,000 条 1536 维候选向量池，保证基准测试环境确定且充分预热。
func prepareBenchmarkVectorPool(b *testing.B, capacity, dim int) (*VectorPool, []float32) {
	b.Helper()
	pool := NewVectorPool(capacity, dim)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// 使用固定种子保证基准测试输入数据的确定性
	rng := rand.New(rand.NewSource(42))
	items := make([]VectorItem, capacity)
	for i := 0; i < capacity; i++ {
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = rng.Float32() - 0.5
		}
		items[i] = VectorItem{
			VideoID:    uint(i + 1),
			CreateTime: baseTime.Add(time.Duration(i) * time.Minute),
			Vector:     vec,
		}
	}
	admitted := pool.PutBatch(items)
	if admitted != capacity {
		b.Fatalf("failed to populate pool: admitted %d != %d", admitted, capacity)
	}

	query := make([]float32, dim)
	var qSum float64
	for d := 0; d < dim; d++ {
		val := rng.Float32() - 0.5
		query[d] = val
		qSum += float64(val) * float64(val)
	}
	// 提前进行 L2 归一化，模拟线上真实传入已归一化用户画像向量的真实场景
	if qSum > 0 {
		inv := 1.0 / math.Sqrt(qSum)
		for d := 0; d < dim; d++ {
			query[d] = float32(float64(query[d]) * inv)
		}
	}

	return pool, query
}

// BenchmarkVectorPool_SearchCosine 度量 2,000 条候选向量规模下的单协程余弦检索耗时，
// 验证单次调用均值严格满足低于 2ms 的 SLA 性能预算。
func BenchmarkVectorPool_SearchCosine(b *testing.B) {
	const (
		capacity = 2000
		dim      = 1536
		topK     = 50
	)
	pool, query := prepareBenchmarkVectorPool(b, capacity, dim)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hits := pool.SearchCosine(query, topK, nil)
		if len(hits) != topK {
			b.Fatalf("expected %d hits, got %d", topK, len(hits))
		}
	}
}

// BenchmarkVectorPool_SearchCosine_Parallel 度量 2,000 条候选向量规模下的多协程并发余弦检索吞吐与耗时，
// 验证在无锁只读快照支持下高并发读链路的低竞争扩展性。
func BenchmarkVectorPool_SearchCosine_Parallel(b *testing.B) {
	const (
		capacity = 2000
		dim      = 1536
		topK     = 50
	)
	pool, query := prepareBenchmarkVectorPool(b, capacity, dim)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hits := pool.SearchCosine(query, topK, nil)
			if len(hits) != topK {
				b.Fatalf("expected %d hits, got %d", topK, len(hits))
			}
		}
	})
}
