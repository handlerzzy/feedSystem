package embed

import (
	"testing"
)

// BenchmarkEncode_1536Dim 度量文本词法哈希生成 1536 维向量的标准吞吐，
// 覆盖典型中文短标题、中长技术标题以及中英混合长文本。
func BenchmarkEncode_1536Dim(b *testing.B) {
	cases := []struct {
		name string
		text string
	}{
		{
			name: "ShortTitle",
			text: "猫咪日常：橘猫的一天",
		},
		{
			name: "TechnicalMedium",
			text: "Go 并发编程：goroutine 与 channel 原理深度剖析",
		},
		{
			name: "MixedLong",
			text: "分布式系统的一致性与高可用架构实战：从 Raft 协议到 Paxos 2026 最新最佳实践指南",
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				vec := Encode(tc.text)
				if len(vec) != LexicalDim {
					b.Fatalf("expected dim %d, got %d", LexicalDim, len(vec))
				}
			}
		})
	}
}

// BenchmarkLexicalUnits 度量分词与 Bigram 词法切分单元的性能。
func BenchmarkLexicalUnits(b *testing.B) {
	text := "Go 并发编程：goroutine 与 channel 原理深度剖析"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		units := LexicalUnits(text)
		if len(units) == 0 {
			b.Fatal("unexpected empty units")
		}
	}
}

// BenchmarkFNV1a 度量核心 32 位 FNV-1a 字符串哈希计算吞吐。
func BenchmarkFNV1a(b *testing.B) {
	text := "goroutine与channel"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = FNV1a(text)
	}
}
