#!/usr/bin/env bash
# ==============================================================================
# run_benchmark.sh —— 根目录一键式微基准测试与性能防退化审查入口
# ==============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/test/benchmark/run.sh" "$@"
