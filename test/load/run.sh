#!/usr/bin/env bash
# ==============================================================================
# run.sh —— 容器化 k6 宏观并发压测一键式启动脚本（含 pprof 联动诊断与阈值门禁）
#
# 功能：
#   1. 检查并确保 Docker 微服务集群网络与容器就绪；
#   2. 自动检查/预置带有真实点赞历史与凭据的测试身份池；
#   3. 使用官方 grafana/k6 容器接入微服务内部网络 (feedflow_default) 发压；
#   4. 支持阶梯并发加压 (50 ➔ 200 ➔ 500 VU) 与轻量快速冒烟校验 (--smoke)；
#   5. 支持联动 pprof 运行时性能分析采集 (--profile)；
#   6. 严格执行 SLA 阈值门禁，防止静默吞掉压测失败 (支持 --ignore-thresholds)。
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NETWORK_NAME="${NETWORK_NAME:-feedflow_default}"
BASE_URL="${BASE_URL:-http://backend:8080}"
PPROF_URL="${PPROF_URL:-http://backend:6060}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:latest}"

SMOKE=false
SEED_ONLY=false
SKIP_SEED=false
PROFILE=false
IGNORE_THRESHOLDS=false
USERS_COUNT="${USERS_COUNT:-500}"
VIDEOS_COUNT="${VIDEOS_COUNT:-100}"
PACING_MS="${PACING_MS:-20}"
PROFILE_SECONDS="${PROFILE_SECONDS:-}"
THRESHOLD_P99="${THRESHOLD_P99:-}"
THRESHOLD_P95="${THRESHOLD_P95:-}"
NO_CACHE=false

usage() {
  cat << EOF
FeedFlow 容器化并发压测套件启动器

用法:
  $0 [选项]

选项:
  --smoke                运行 3 秒极轻量冒烟测试（1 VU），用于快速验证链路可用性
  --no-cache             关闭后端 L1 进程内缓存，直接压测全链路直穿性能 (FEED_LOCAL_CACHE_ENABLED=false)
  --seed-only            仅执行测试账号预热与凭据数据生成，不发起压测
  --skip-seed            若 tokens.json 已存在，跳过数据预热阶段
  --profile              在压测活跃期间自动抓取 CPU 火焰图与 Heap 快照（存至 test/load/profiles/）
  --profile-seconds <N>  CPU profile 采样时长秒数（默认：全量 15s，冒烟 2s）
  --ignore-thresholds    当 k6 触发 SLA 阈值红线告警 (Exit code 99) 时仅告警不阻断返回 0
  --threshold-p99 <ms>   自定义 P99 SLA 判定阈值毫秒数
  --threshold-p95 <ms>   自定义 P95 SLA 判定阈值毫秒数
  --users <N>            预置压测账号数量 (默认: 500)
  --videos <N>           候选视频池大小 (默认: 100)
  --pacing <ms>          单个 VU 两次请求间的思考间隔毫秒数 (默认: 20)
  -h, --help             显示此帮助信息

示例:
  $0 --smoke                           # 冒烟验证
  $0                                   # 执行完整阶梯并发压测 (50 ➔ 200 ➔ 500 VU)
  $0 --no-cache                        # 关闭 L1 缓存进行穿透压测
  $0 --profile                         # 压测并在高负载阶段抓取 pprof 火焰图快照
  $0 --profile --ignore-thresholds     # 探索性压测抓取火焰图，放行阈值告警
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke)
      SMOKE=true
      shift
      ;;
    --no-cache)
      NO_CACHE=true
      export FEED_LOCAL_CACHE_ENABLED=false
      shift
      ;;
    --seed-only)
      SEED_ONLY=true
      shift
      ;;
    --skip-seed)
      SKIP_SEED=true
      shift
      ;;
    --profile)
      PROFILE=true
      shift
      ;;
    --profile-seconds)
      PROFILE_SECONDS="$2"
      shift 2
      ;;
    --ignore-thresholds)
      IGNORE_THRESHOLDS=true
      shift
      ;;
    --threshold-p99)
      THRESHOLD_P99="$2"
      shift 2
      ;;
    --threshold-p95)
      THRESHOLD_P95="$2"
      shift 2
      ;;
    --users)
      USERS_COUNT="$2"
      shift 2
      ;;
    --videos)
      VIDEOS_COUNT="$2"
      shift 2
      ;;
    --pacing)
      PACING_MS="$2"
      shift 2
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

if [ -z "$PROFILE_SECONDS" ]; then
  if [ "$SMOKE" = true ]; then
    PROFILE_SECONDS=2
  else
    PROFILE_SECONDS=15
  fi
fi

echo "================================================================="
echo "        FeedFlow 容器化 k6 宏观端到端性能压测套件               "
echo "================================================================="
echo "微服务内网:   $NETWORK_NAME"
echo "目标基准网关: $BASE_URL"
echo "pprof 诊断端口: $PPROF_URL"
echo "压测镜像:     $K6_IMAGE"
echo "运行模式:     $([ "$SMOKE" = true ] && echo "冒烟模式 (1 VU 校验)" || echo "全量阶梯加压 (50 ➔ 200 ➔ 500 VU)")"
echo "L1 本地缓存:  $([ "$NO_CACHE" = true ] && echo "已禁用 (--no-cache，直穿 MySQL/Redis)" || echo "已启用 (FEED_LOCAL_CACHE_ENABLED=true)")"
echo "性能诊断联动: $([ "$PROFILE" = true ] && echo "已开启 (采样 ${PROFILE_SECONDS}s CPU/Heap)" || echo "未开启")"
echo "阈值拦截策略: $([ "$IGNORE_THRESHOLDS" = true ] && echo "放行阈值告警 (--ignore-thresholds)" || echo "严格门禁 (SLA 不满足时非零退出)")"
echo "-----------------------------------------------------------------"

# 1. 检查 Docker 及网络环境
if ! command -v docker >/dev/null 2>&1; then
  echo "❌ 未检测到 Docker 运行环境，请先启动 Docker 服务"
  exit 1
fi

if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "❌ Docker 网络 $NETWORK_NAME 不存在，请先执行 docker compose up -d 启动集群"
  exit 1
fi

# 同步检查并保证后端容器 L1 缓存环境变量与当前压测设置一致
if docker ps --format '{{.Names}}' | grep -q "^feedflow-backend-1$"; then
  target_cache_env="true"
  if [ "$NO_CACHE" = true ]; then
    target_cache_env="false"
  fi
  current_cache_env=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' feedflow-backend-1 2>/dev/null | grep '^FEED_LOCAL_CACHE_ENABLED=' | cut -d= -f2 || true)
  if [ "$current_cache_env" != "$target_cache_env" ]; then
    echo "🔄 同步更新 backend 容器 L1 缓存设置 (FEED_LOCAL_CACHE_ENABLED=$target_cache_env)..."
    FEED_LOCAL_CACHE_ENABLED="$target_cache_env" docker compose -f "$REPO_ROOT/docker-compose.yml" up -d --no-deps backend
    echo "⏳ 等待 backend 服务重新就绪..."
    for i in $(seq 1 30); do
      if [ "$(docker inspect -f '{{.State.Health.Status}}' feedflow-backend-1 2>/dev/null)" = "healthy" ]; then
        break
      fi
      sleep 1
    done
  fi
fi

# 2. 检查或执行压测数据预置
TOKENS_FILE="$SCRIPT_DIR/tokens.json"
if [ "$SKIP_SEED" = false ] || [ ! -f "$TOKENS_FILE" ]; then
  echo "🔍 检查并预置压测测试数据 (用户池: $USERS_COUNT, 候选视频: $VIDEOS_COUNT)..."
  USERS_COUNT="$USERS_COUNT" VIDEOS_COUNT="$VIDEOS_COUNT" "$SCRIPT_DIR/seed_users.sh"
else
  echo "⏩ 检测到既有凭据池文件 ($TOKENS_FILE)，跳过预热阶段"
fi

if [ "$SEED_ONLY" = true ]; then
  echo "✅ 账号数据与凭据预热完毕，退出（--seed-only 模式）"
  exit 0
fi

# 3. 组织压测环境参数
K6_ENV_ARGS=(-e BASE_URL="$BASE_URL")
if [ "$SMOKE" = true ]; then
  K6_ENV_ARGS+=(-e SMOKE="true")
fi
if [ -n "$PACING_MS" ]; then
  K6_ENV_ARGS+=(-e PACING_MS="$PACING_MS")
fi
if [ -n "$THRESHOLD_P99" ]; then
  K6_ENV_ARGS+=(-e THRESHOLD_P99="$THRESHOLD_P99")
fi
if [ -n "$THRESHOLD_P95" ]; then
  K6_ENV_ARGS+=(-e THRESHOLD_P95="$THRESHOLD_P95")
fi
if [ -n "${TARGET_1:-}" ]; then K6_ENV_ARGS+=(-e TARGET_1="$TARGET_1"); fi
if [ -n "${TARGET_2:-}" ]; then K6_ENV_ARGS+=(-e TARGET_2="$TARGET_2"); fi
if [ -n "${TARGET_3:-}" ]; then K6_ENV_ARGS+=(-e TARGET_3="$TARGET_3"); fi
for var in STAGE_1_RAMP STAGE_1_HOLD STAGE_2_RAMP STAGE_2_HOLD STAGE_3_RAMP STAGE_3_HOLD STAGE_COOLDOWN; do
  if [ -n "${!var:-}" ]; then
    K6_ENV_ARGS+=(-e "$var=${!var}")
  fi
done

PROFILES_DIR="$SCRIPT_DIR/profiles"
PROFILE_PID=""

# 4. 若开启 --profile，启动后台采样协程
if [ "$PROFILE" = true ]; then
  mkdir -p "$PROFILES_DIR"
  echo "🔍 [pprof] 已启动性能分析采样监听，将在加压活跃期向 $PPROF_URL 抓取快照..."
  (
    # 等待压测请求进入稳定期
    if [ "$SMOKE" = true ]; then
      sleep 1
    else
      sleep 8
    fi
    TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
    CPU_FILE="$PROFILES_DIR/cpu_${TIMESTAMP}.pprof"
    HEAP_FILE="$PROFILES_DIR/heap_${TIMESTAMP}.pprof"
    GOROUTINE_FILE="$PROFILES_DIR/goroutine_${TIMESTAMP}.txt"

    echo "⏱️  [pprof] 正在高并发下采集 CPU Profile (${PROFILE_SECONDS}s) 与 Heap Snapshot..."

    # 优先尝试宿主机直接访问（若 6060 端口映射成功），兜底走 Docker 内网容器直连
    if command -v curl >/dev/null 2>&1 && curl -s -f --connect-timeout 2 "http://127.0.0.1:6060/debug/pprof/" >/dev/null 2>&1; then
      curl -s "http://127.0.0.1:6060/debug/pprof/profile?seconds=${PROFILE_SECONDS}" -o "$CPU_FILE" &
      PID_CPU=$!
      curl -s "http://127.0.0.1:6060/debug/pprof/heap" -o "$HEAP_FILE" || true
      curl -s "http://127.0.0.1:6060/debug/pprof/goroutine?debug=1" -o "$GOROUTINE_FILE" || true
      wait $PID_CPU || true
    else
      docker run --rm \
        --network "$NETWORK_NAME" \
        -v "$PROFILES_DIR:/profiles" \
        alpine:3.21 sh -c "
          wget -qO /profiles/heap_${TIMESTAMP}.pprof '${PPROF_URL}/debug/pprof/heap' || true
          wget -qO /profiles/goroutine_${TIMESTAMP}.txt '${PPROF_URL}/debug/pprof/goroutine?debug=1' || true
          wget -qO /profiles/cpu_${TIMESTAMP}.pprof '${PPROF_URL}/debug/pprof/profile?seconds=${PROFILE_SECONDS}' || true
        " >/dev/null 2>&1 || echo "⚠️ [pprof] 容器直连采样失败，请检查 backend 容器 pprof 服务状态"
    fi

    if [ -f "$CPU_FILE" ] && [ -s "$CPU_FILE" ]; then
      echo "✅ [pprof] 采样成功归档至:"
      echo "   - CPU Profile:  $CPU_FILE"
      echo "   - Heap Profile: $HEAP_FILE"
      echo "   - Goroutine:    $GOROUTINE_FILE"
    fi
  ) &
  PROFILE_PID=$!
fi

# 5. 容器化启动 k6 发起压测
echo "🚀 启动 grafana/k6 容器接入内网执行发压..."
K6_EXIT_CODE=0
docker run --rm \
  --network "$NETWORK_NAME" \
  -v "$SCRIPT_DIR:/test" \
  "${K6_ENV_ARGS[@]}" \
  "$K6_IMAGE" run /test/k6_feed.js || K6_EXIT_CODE=$?

# 等待后台采样完成
if [ -n "$PROFILE_PID" ]; then
  wait "$PROFILE_PID" 2>/dev/null || true
fi

chmod 666 "$SCRIPT_DIR"/summary.* "$SCRIPT_DIR"/profiles/* 2>/dev/null || true

echo "-----------------------------------------------------------------"
echo "📊 压测报告已归档:"
echo "   - Markdown 看板: $SCRIPT_DIR/summary.md"
echo "   - JSON 指标快照: $SCRIPT_DIR/summary.json"
if [ "$PROFILE" = true ]; then
  echo "   - pprof 火焰图快照目录: $PROFILES_DIR"
  echo "   💡 建议使用以下命令可视化分析瓶颈:"
  echo "      go tool pprof -http=:8081 $(ls -t "$PROFILES_DIR"/cpu_*.pprof 2>/dev/null | head -n 1 || echo '<cpu.pprof>')"
fi
echo "================================================================="
if [ -f "$SCRIPT_DIR/summary.md" ]; then
  cat "$SCRIPT_DIR/summary.md"
fi

# 6. 处理退出码（严格门禁，除非显式指定 --ignore-thresholds）
if [ "$K6_EXIT_CODE" -ne 0 ]; then
  if [ "$K6_EXIT_CODE" -eq 99 ] && [ "$IGNORE_THRESHOLDS" = true ]; then
    echo "⚠️ 压测指标触发 SLA 告警阈值 (Exit code 99)，已指定 --ignore-thresholds，放行返回成功。"
    exit 0
  fi
  echo "❌ 压测执行失败或未满足 SLA 阈值红线 (Exit code: $K6_EXIT_CODE)"
  exit "$K6_EXIT_CODE"
fi

echo "🎉 恭喜！全链路压测执行完毕且所有 SLA 门禁达标通过！"
