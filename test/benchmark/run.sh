#!/usr/bin/env bash
# ==============================================================================
# run.sh —— 核心算法微基准测试套件启动器与性能防退化审查工具
#
# 功能：
#   1. 轻量级冒烟验证 (--smoke)：快速执行 1 次迭代，校验基准测试代码签名、
#      可编译性与运行期零 panic，用于本地与 CI 流水线；
#   2. 基准采样建档 (--record [文件路径])：运行 5 轮统计采样并落盘官方快照；
#   3. 性能退化对比 (--compare [基线文件] [待测文件])：对比新旧版本的耗时与内存分配，
#      自动调用 benchstat（若已安装）与内置 SLA / 0 堆逃逸门禁分析器。
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BACKEND_DIR="$REPO_ROOT/backend"
DEFAULT_BASELINE="$SCRIPT_DIR/baseline.txt"

# 自动定位 Go 二进制
if command -v go >/dev/null 2>&1; then
  GO_BIN="go"
elif [ -x "/usr/local/go/bin/go" ]; then
  GO_BIN="/usr/local/go/bin/go"
elif [ -x "$HOME/go/bin/go" ]; then
  GO_BIN="$HOME/go/bin/go"
else
  echo "❌ 未检测到 Go 环境，请先安装 Go 或将其加入 PATH"
  exit 1
fi

MODE="smoke"
RECORD_TARGET=""
COMPARE_BASE=""
COMPARE_CAND=""

usage() {
  cat << EOF
FeedFlow 算法性能基准测试与防退化审查套件

用法:
  $0 [选项]

选项:
  --smoke                       (默认) 运行基准测试 smoke 快速验证（-benchtime=1x），
                                校验基准函数可编译且无崩溃，适合本地提交前与 CI
  --record [文件路径]            运行多轮基准测试（-count=5）并将官方快照写入指定文件
                                (缺省路径: test/benchmark/baseline.txt)
  --compare <基线> [待测文件]    比对基准测试结果差值。若未指定待测文件，则现场运行测试生成临时对比数据
  -h, --help                    显示此帮助信息

示例:
  $0 --smoke                                  # 执行 CI 级轻量 smoke 验证
  $0 --record                                 # 采集并覆盖官方 baseline.txt 快照
  $0 --compare test/benchmark/baseline.txt    # 采集当前代码并与官方基线比对审查
  $0 --compare baseline.txt new.txt           # 对比已有的两份基准输出文件
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke)
      MODE="smoke"
      shift
      ;;
    --record)
      MODE="record"
      shift
      if [[ $# -gt 0 && ! "$1" =~ ^-- ]]; then
        RECORD_TARGET="$1"
        shift
      fi
      ;;
    --compare)
      MODE="compare"
      shift
      if [[ $# -gt 0 && ! "$1" =~ ^-- ]]; then
        COMPARE_BASE="$1"
        shift
      fi
      if [[ $# -gt 0 && ! "$1" =~ ^-- ]]; then
        COMPARE_CAND="$1"
        shift
      fi
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "❌ 未知参数: $1"
      usage
      ;;
  esac
done

to_abs_path() {
  local p="$1"
  if [[ "$p" = /* ]]; then
    echo "$p"
  else
    echo "$(pwd)/$p"
  fi
}

[ -n "$RECORD_TARGET" ] && RECORD_TARGET="$(to_abs_path "$RECORD_TARGET")"
[ -n "$COMPARE_BASE" ] && COMPARE_BASE="$(to_abs_path "$COMPARE_BASE")"
[ -n "$COMPARE_CAND" ] && COMPARE_CAND="$(to_abs_path "$COMPARE_CAND")"

cd "$BACKEND_DIR"

if [ "$MODE" = "smoke" ]; then
  echo "================================================================="
  echo "        FeedFlow 微观算法基准测试 Smoke 验证 (CI 快速校验)        "
  echo "================================================================="
  echo "执行命令: $GO_BIN test -run='^$' -bench='^Benchmark' -benchmem -benchtime=1x ./..."
  echo "-----------------------------------------------------------------"
  $GO_BIN test -run='^$' -bench='^Benchmark' -benchmem -benchtime=1x ./...
  echo "-----------------------------------------------------------------"
  echo "✅ 基准测试 Smoke 验证全量通过：所有基准测试签名正确，运行期无 Panic，无内存崩溃。"
  exit 0
fi

if [ "$MODE" = "record" ]; then
  OUT_FILE="${RECORD_TARGET:-$DEFAULT_BASELINE}"
  echo "================================================================="
  echo "        FeedFlow 微观算法基准测试指标采样 (生成官方快照)         "
  echo "================================================================="
  echo "目标文件: $OUT_FILE"
  echo "采样轮数: 5 轮 (count=5)"
  echo "执行命令: $GO_BIN test -run='^$' -bench='^Benchmark' -benchmem -count=5 ./internal/video ./internal/feed ./internal/embed"
  echo "-----------------------------------------------------------------"
  mkdir -p "$(dirname "$OUT_FILE")"
  $GO_BIN test -run='^$' -bench='^Benchmark' -benchmem -count=5 ./internal/video ./internal/feed ./internal/embed | tee "$OUT_FILE"
  echo "-----------------------------------------------------------------"
  echo "✅ 官方基准快照已成功落盘至: $OUT_FILE"
  exit 0
fi

if [ "$MODE" = "compare" ]; then
  BASE_FILE="${COMPARE_BASE:-$DEFAULT_BASELINE}"
  if [ ! -f "$BASE_FILE" ]; then
    echo "❌ 基线文件不存在: $BASE_FILE"
    echo "提示: 请先运行 '$0 --record' 沉淀基线文件，或指定有效的基线路径。"
    exit 1
  fi

  TMP_CAND=""
  if [ -z "$COMPARE_CAND" ]; then
    TMP_CAND="$(mktemp /tmp/feedflow_bench_XXXXXX.txt)"
    echo "🔍 现场采样当前代码基准指标 (count=5)..."
    $GO_BIN test -run='^$' -bench='^Benchmark' -benchmem -count=5 ./internal/video ./internal/feed ./internal/embed > "$TMP_CAND"
    CAND_FILE="$TMP_CAND"
  else
    CAND_FILE="$COMPARE_CAND"
    if [ ! -f "$CAND_FILE" ]; then
      echo "❌ 待测基准文件不存在: $CAND_FILE"
      exit 1
    fi
  fi

  # 1. 尝试使用官方 benchstat 工具（如果已安装）
  BENCHSTAT_BIN=""
  if command -v benchstat >/dev/null 2>&1; then
    BENCHSTAT_BIN="benchstat"
  elif [ -x "$("$GO_BIN" env GOPATH 2>/dev/null)/bin/benchstat" ]; then
    BENCHSTAT_BIN="$("$GO_BIN" env GOPATH)/bin/benchstat"
  fi

  if [ -n "$BENCHSTAT_BIN" ]; then
    echo
    echo "📊 执行官方 benchstat 统计学显著性分析:"
    "$BENCHSTAT_BIN" "$BASE_FILE" "$CAND_FILE" || true
    echo
  fi

  # 2. 调用内置门禁分析器（进行 SLA 性能预算与 0 堆逃逸断言检测）
  EXIT_CODE=0
  if command -v python3 >/dev/null 2>&1; then
    python3 "$SCRIPT_DIR/compare.py" "$BASE_FILE" "$CAND_FILE" || EXIT_CODE=$?
  fi

  # 清理临时文件
  if [ -n "$TMP_CAND" ] && [ -f "$TMP_CAND" ]; then
    rm -f "$TMP_CAND"
  fi

  exit "$EXIT_CODE"
fi
