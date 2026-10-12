#!/usr/bin/env bash
# ==============================================================================
# run_load_test.sh —— 根目录一键式并发压测执行快捷入口
#
# 支持参数：
#   --no-cache   关闭后端 L1 进程内缓存，直接压测全链路直穿性能
#   其他选项均直接透传至 test/load/run.sh
# ==============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for arg in "$@"; do
  if [ "$arg" = "--no-cache" ]; then
    export FEED_LOCAL_CACHE_ENABLED=false
  fi
done
exec "$SCRIPT_DIR/test/load/run.sh" "$@"
