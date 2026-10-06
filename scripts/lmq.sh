#!/usr/bin/env bash
# 本地消息队列命令行入口，方便脚本复现。
# 用法: scripts/lmq.sh <command> [args]
#   publish  <topic> <body> [dedupKey]     投递（返回 created=true/false 区分新入队/去重）
#   groups   <group>                        注册消费组
#   poll     <group> [consumer]             领取一条消息（含 deliveryId/status 等）
#   ack      <deliveryId> [consumer]        确认成功
#   reject   <deliveryId> [consumer] [err]  拒绝（触发退避重试或进死信）
#   status   <deliveryId>                   查询单条投递状态
#   dead     <group>                        查看死信列表
#   replay   <deliveryId>                   手动重放死信
#   stats    <group>                        查询各组状态数量（含 total，校验守恒）
#   config                                 查看生效配置
set -euo pipefail
BASE="${LMQ_BASE:-http://localhost:8080/api/queue}"
cmd="${1:?command required}"; shift || true
j() { python3 -c 'import sys,json;print(json.dumps(json.load(sys.stdin),ensure_ascii=False,indent=2))'; }
case "$cmd" in
  publish)
    topic="$1"; body="$2"; dedup="${3-}"
    if [ -n "$dedup" ]; then payload=$(python3 -c 'import json,sys;print(json.dumps({"body":sys.argv[1],"dedupKey":sys.argv[2]}))' "$body" "$dedup")
    else payload=$(python3 -c 'import json,sys;print(json.dumps({"body":sys.argv[1]}))' "$body"); fi
    curl -sS -X POST "$BASE/topics/$topic/messages" -H 'Content-Type: application/json' -d "$payload" | j ;;
  groups) curl -sS -X POST "$BASE/groups/$1" | j ;;
  poll)   curl -sS -X POST "$BASE/groups/$1/poll?consumer=${2:-cli-consumer}" | j ;;
  ack)    curl -sS -X POST "$BASE/deliveries/$1/ack?consumer=${2:-cli-consumer}" | j ;;
  reject) curl -sS -X POST "$BASE/deliveries/$1/reject?consumer=${2:-cli-consumer}" \
            -H 'Content-Type: application/json' -d "{\"error\":\"${3:-rejected by cli}\"}" | j ;;
  status) curl -sS "$BASE/deliveries/$1" | j ;;
  dead)   curl -sS "$BASE/groups/$1/dead" | j ;;
  replay) curl -sS -X POST "$BASE/deliveries/$1/replay" | j ;;
  stats)  curl -sS "$BASE/groups/$1/stats" | j ;;
  config) curl -sS "$BASE/config" | j ;;
  *) echo "unknown command: $cmd" >&2; exit 2 ;;
esac
