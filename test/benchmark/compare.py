#!/usr/bin/env python3
"""
FeedFlow 基准测试对比分析工具 (Benchmark Comparison Tool)

功能：
  1. 解析 Go 标准 benchmark 输出文件（支持 -count=N 多轮采样）；
  2. 计算 baseline 与 candidate 的各项指标均值（ns/op, B/op, allocs/op）；
  3. 比对指标差值与变化百分比；
  4. 校验 SLA 性能预算与零堆内存逃逸门禁（Zero-Allocation Gate）。
"""

import sys
import re
from collections import defaultdict

# SLA 性能预算配置（单位：纳秒 ns）
SLA_BUDGETS_NS = {
    "BenchmarkVectorPool_SearchCosine": 2_000_000,            # 2ms
    "BenchmarkCalculateLocalRerankScore_50Candidates": 100_000, # 100μs (0.1ms)
}

# 强制零堆逃逸断言（0 allocs/op, 0 B/op）
ZERO_ALLOC_BENCHMARKS = {
    "BenchmarkCalculateLocalRerankScore_50Candidates",
    "BenchmarkFNV1a",
}

class BenchResult:
    def __init__(self, name):
        self.name = name
        self.times_ns = []
        self.bytes_op = []
        self.allocs_op = []

    def add(self, time_ns, b_op, a_op):
        self.times_ns.append(time_ns)
        if b_op is not None:
            self.bytes_op.append(b_op)
        if a_op is not None:
            self.allocs_op.append(a_op)

    @property
    def avg_time_ns(self):
        return sum(self.times_ns) / len(self.times_ns) if self.times_ns else 0.0

    @property
    def avg_bytes(self):
        return sum(self.bytes_op) / len(self.bytes_op) if self.bytes_op else 0.0

    @property
    def avg_allocs(self):
        return sum(self.allocs_op) / len(self.allocs_op) if self.allocs_op else 0.0


def parse_benchmark_file(filepath):
    """解析 Go benchmark 输出文件"""
    results = {}
    # 格式示例：BenchmarkCalculateLocalRerankScore_50Candidates-20   171397   7837 ns/op   0 B/op   0 allocs/op
    pattern = re.compile(r"^(Benchmark[^\s\-/]+(?:/[^\s\-]+)?)(?:-\d+)?\s+\d+\s+([\d\.]+)\s+ns/op(?:\s+([\d\.]+)\s+B/op)?(?:\s+([\d\.]+)\s+allocs/op)?")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                match = pattern.match(line)
                if match:
                    raw_name = match.group(1)
                    time_ns = float(match.group(2))
                    bytes_op = float(match.group(3)) if match.group(3) is not None else None
                    allocs_op = float(match.group(4)) if match.group(4) is not None else None

                    if raw_name not in results:
                        results[raw_name] = BenchResult(raw_name)
                    results[raw_name].add(time_ns, bytes_op, allocs_op)
    except Exception as e:
        print(f"❌ 读取文件失败 {filepath}: {e}", file=sys.stderr)
        sys.exit(1)

    return results


def format_time(ns):
    if ns < 1_000:
        return f"{ns:.1f}ns"
    elif ns < 1_000_000:
        return f"{ns/1_000:.2f}μs"
    elif ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f}ms"
    else:
        return f"{ns/1_000_000_000:.2f}s"


def format_delta(base, cand):
    if base == 0:
        if cand == 0:
            return "+0.00%"
        return "+inf%"
    pct = ((cand - base) / base) * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def main():
    if len(sys.argv) < 3:
        print("用法: python3 compare.py <baseline.txt> <candidate.txt>", file=sys.stderr)
        sys.exit(1)

    base_path = sys.argv[1]
    cand_path = sys.argv[2]

    base_results = parse_benchmark_file(base_path)
    cand_results = parse_benchmark_file(cand_path)

    if not base_results:
        print(f"⚠️ 在基线文件 {base_path} 中未解析到有效的 Benchmark 数据", file=sys.stderr)
        sys.exit(1)
    if not cand_results:
        print(f"⚠️ 在待检文件 {cand_path} 中未解析到有效的 Benchmark 数据", file=sys.stderr)
        sys.exit(1)

    # ANSI 样式
    C_RED = "\033[31m"
    C_GRN = "\033[32m"
    C_YEL = "\033[33m"
    C_BLD = "\033[1m"
    C_CYN = "\033[36m"
    C_RST = "\033[0m"

    print(f"\n{C_BLD}{C_CYN}========================================================================================={C_RST}")
    print(f"{C_BLD}                         FeedFlow 微观算法性能基准比对审查看板                           {C_RST}")
    print(f"{C_BLD}{C_CYN}========================================================================================={C_RST}")
    print(f" 基线快照 (Old): {base_path}")
    print(f" 当前改动 (New): {cand_path}")
    print(f"-----------------------------------------------------------------------------------------")

    headers = f"{'基准测试项 (Benchmark)':<42} {'基线耗时':<10} {'待测耗时':<10} {'耗时变化':<9} {'内存分配':<12} {'堆逃逸变化':<12} {'门禁裁定'}"
    print(f"{C_BLD}{headers}{C_RST}")
    print("-" * 115)

    has_violation = False
    has_warning = False

    all_keys = sorted(set(list(base_results.keys()) + list(cand_results.keys())))

    for name in all_keys:
        if name not in base_results:
            print(f"{name:<42} {'[新增]':<10} {format_time(cand_results[name].avg_time_ns):<10} {'-':<9} {'-':<12} {'-':<12} {C_GRN}✅ NEW{C_RST}")
            continue
        if name not in cand_results:
            print(f"{name:<42} {format_time(base_results[name].avg_time_ns):<10} {'[缺失]':<10} {'-':<9} {'-':<12} {'-':<12} {C_RED}❌ MISSING{C_RST}")
            has_violation = True
            continue

        b = base_results[name]
        c = cand_results[name]

        time_delta_str = format_delta(b.avg_time_ns, c.avg_time_ns)
        mem_str = f"{c.avg_bytes:.0f} B"
        allocs_delta_str = f"{b.avg_allocs:.0f} ➔ {c.avg_allocs:.0f}"

        # 判定
        verdict = f"{C_GRN}✅ PASS{C_RST}"
        status_msg = ""

        # 1. 零逃逸断言检测
        if name in ZERO_ALLOC_BENCHMARKS:
            if c.avg_allocs > 0 or c.avg_bytes > 0:
                verdict = f"{C_RED}❌ ESCAPE{C_RST}"
                status_msg = f" (破坏零堆逃逸门禁: {c.avg_allocs} allocs)"
                has_violation = True

        # 2. SLA 预算检测
        if name in SLA_BUDGETS_NS:
            budget = SLA_BUDGETS_NS[name]
            if c.avg_time_ns > budget:
                verdict = f"{C_RED}❌ SLA_FAIL{C_RST}"
                status_msg = f" (突破SLA预算 {format_time(budget)})"
                has_violation = True

        # 3. 耗时严重劣化检测（持续超过 +15% 报警）
        if b.avg_time_ns > 0:
            pct = ((c.avg_time_ns - b.avg_time_ns) / b.avg_time_ns) * 100
            if pct > 15.0 and verdict != f"{C_RED}❌ ESCAPE{C_RST}":
                if not has_violation:
                    verdict = f"{C_YEL}⚠️  SLOW{C_RST}"
                    status_msg = f" (劣化超过15%)"
                    has_warning = True

        print(f"{name:<42} {format_time(b.avg_time_ns):<10} {format_time(c.avg_time_ns):<10} {time_delta_str:<9} {mem_str:<12} {allocs_delta_str:<12} {verdict}{status_msg}")

    print("-" * 115)
    if has_violation:
        print(f"{C_RED}{C_BLD}❌ 审查结论: 存在性能硬性退化或零逃逸门禁破坏，请阻断合并并按上述指标修复！{C_RST}\n")
        sys.exit(1)
    elif has_warning:
        print(f"{C_YEL}{C_BLD}⚠️  审查结论: 存在明显耗时退化（>15%），请作者确认环境负载波动或在 PR 说明中附带合理性说明。{C_RST}\n")
        sys.exit(0)
    else:
        print(f"{C_GRN}{C_BLD}✅ 审查结论: 全部基准指标正常，SLA 与零逃逸门禁全部通过！{C_RST}\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
