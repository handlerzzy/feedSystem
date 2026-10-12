#!/usr/bin/env bash
# ==============================================================================
# seed_users.sh —— 压测账号与点赞流量预热数据预置脚本
#
# 功能：
#   1. 向系统预置具备历史互动点赞行为的压测测试账号池（默认 500 个账号）；
#   2. 为候选视频池计算并补充 1536 维词法向量与时间线缓存；
#   3. 生成并导出 tokens.json 供 k6 压测套件挂载消费；
#   4. 清理 Redis 限流缓存，确保全链路并发发压畅通无阻。
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

USERS_COUNT="${USERS_COUNT:-500}"
VIDEOS_COUNT="${VIDEOS_COUNT:-100}"
TOKENS_OUT="${TOKENS_OUT:-$SCRIPT_DIR/tokens.json}"
NETWORK_NAME="${NETWORK_NAME:-feedflow_default}"

echo "================================================================="
echo "   FeedFlow 压测数据与身份凭据预置 (Pre-warm & Data Seeding)     "
echo "================================================================="
echo "目标压测账号数: $USERS_COUNT"
echo "候选视频池大小: $VIDEOS_COUNT"
echo "凭据输出文件:   $TOKENS_OUT"
echo "容器共享网络:   $NETWORK_NAME"
echo "-----------------------------------------------------------------"

# 1. 确保二进制存在
if [ ! -f "$SCRIPT_DIR/seedload" ]; then
  if command -v go >/dev/null 2>&1; then
    echo "⚡ 正在本地编译 seedload 工具..."
    (cd "$REPO_ROOT/backend" && CGO_ENABLED=0 go build -trimpath -o "$SCRIPT_DIR/seedload" ./cmd/seedload)
  elif [ -x "/usr/local/go/bin/go" ]; then
    echo "⚡ 正在使用 /usr/local/go/bin/go 编译 seedload 工具..."
    (cd "$REPO_ROOT/backend" && export PATH=$PATH:/usr/local/go/bin && CGO_ENABLED=0 go build -trimpath -o "$SCRIPT_DIR/seedload" ./cmd/seedload)
  elif command -v docker >/dev/null 2>&1; then
    echo "⚡ 宿主机未检测到 Go，采用容器化 golang:1.24 编译 seedload (自包含环境)..."
    docker run --rm \
      -v "$REPO_ROOT/backend:/src" \
      -v "$SCRIPT_DIR:/out" \
      -w /src \
      golang:1.24 \
      sh -c "CGO_ENABLED=0 go build -trimpath -o /out/seedload ./cmd/seedload"
  else
    echo "❌ 未检测到 Go 或 Docker 运行环境且 $SCRIPT_DIR/seedload 不存在"
    exit 1
  fi
fi

# 2. 容器化执行数据预置（零宿主机网络依赖，直连微服务内网）
echo "🚀 启动容器化数据注入任务..."
docker run --rm \
  --network "$NETWORK_NAME" \
  -v "$SCRIPT_DIR:/test/load" \
  alpine:3.21 \
  /test/load/seedload \
    -users "$USERS_COUNT" \
    -videos "$VIDEOS_COUNT" \
    -out "/test/load/$(basename "$TOKENS_OUT")"

echo "-----------------------------------------------------------------"
echo "✅ 数据预热完成，共生成 $(grep -c '"token"' "$TOKENS_OUT" 2>/dev/null || echo "$USERS_COUNT") 条凭据。"
echo "================================================================="
