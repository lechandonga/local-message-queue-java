#!/usr/bin/env bash
# 一键复现：构建 jar -> 用临时数据目录启动 -> 跑 demo -> 退出时自动关闭服务。
# 用法：./scripts/run-demo.sh [端口]
set -euo pipefail

cd "$(dirname "$0")/.."
PORT="${1:-18099}"
DATA_DIR="$(mktemp -d)/lq-data"
JAR="target/local-message-queue-java-1.0-SNAPSHOT.jar"

echo "==> 构建 $JAR"
mvn -q -DskipTests package
echo "==> 启动服务: port=$PORT data-dir=$DATA_DIR"
java -jar "$JAR" \
  --server.port="$PORT" \
  --lq.data-dir="$DATA_DIR" \
  --lq.retry-backoff-base-ms=200 \
  --lq.retry-backoff-max-ms=2000 &
APP_PID=$!
trap 'kill $APP_PID 2>/dev/null || true' EXIT

for i in $(seq 1 60); do
  curl -s "http://localhost:$PORT/api/queues/stats" 2>/dev/null | grep -q total && break
  sleep 1
done

BASE="http://localhost:$PORT" "$(dirname "$0")/demo.sh"
