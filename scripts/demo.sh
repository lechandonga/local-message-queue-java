#!/usr/bin/env bash
# 本地消息队列演示脚本：投递(含去重) -> 消费 -> 确认/拒绝 -> 重试 -> 死信 -> 重放 -> 统计
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"

req() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$BASE$path" -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -X "$method" "$BASE$path"
  fi
}

# 从 JSON 中提取单个数字/字符串字段，不依赖 jq（列名大小写不敏感）
field_num() { sed -n "s/.*\"$(echo "$1" | tr 'a-z-' 'A-Z_')\":\([0-9][0-9]*\).*/\1/p; t; s/.*\"$1\":\([0-9][0-9]*\).*/\1/p" | head -1; }
field_str() { sed -n "s/.*\"$(echo "$1" | tr 'a-z-' 'A-Z_')\":\"\([^\"]*\)\".*/\1/p; t; s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" | head -1; }

echo "== 1. 首次投递 =="
req POST /api/queues/messages '{"topic":"demo","dedupKey":"d-1","payload":"第一次内容"}'
echo; echo "== 2. 相同 dedupKey 重复投递（应 deduped=true, created=false） =="
req POST /api/queues/messages '{"topic":"demo","dedupKey":"d-1","payload":"重复内容不会生效"}'
echo

echo "== 3. 组 g-a 消费并确认 =="
POLL=$(req POST /api/queues/poll '{"consumerGroup":"g-a","consumerId":"script-1"}')
echo "$POLL"
# 不依赖 jq，用 grep/sed 提取字段
SID=$(echo "$POLL" | field_num groupStateId)
TOKEN=$(echo "$POLL" | field_str leaseToken)
req POST "/api/queues/$SID/ack" "{\"leaseToken\":\"$TOKEN\"}"; echo

echo "== 4. 组 g-b 独立消费，持续拒绝直到进死信 =="
PUBLISH2=$(req POST /api/queues/messages \
  '{"topic":"demo","dedupKey":"d-2","payload":"注定失败的消息"}')
echo "$PUBLISH2"
TARGET_ID=$(echo "$PUBLISH2" | field_num messageId)
SID=""; T=""; P=""
for i in 1 2 3 4; do
  while :; do
    P=$(req POST /api/queues/poll '{"consumerGroup":"g-b","consumerId":"script-2"}')
    MID=$(echo "$P" | field_num messageId)
    S=$(echo "$P" | field_num groupStateId)
    T=$(echo "$P" | field_str leaseToken)
    [[ -z "$MID" ]] && { sleep 0.2; continue; }   # 204 暂无消息，等待退避到期
    if [[ "$MID" == "$TARGET_ID" ]]; then break; fi
    req POST "/api/queues/$S/ack" "{\"leaseToken\":\"$T\"}" >/dev/null  # 跳过组内其他消息
    sleep 0.2
  done
  echo "目标消息第 $i 次投递(attempts 见下): $P"
  req POST "/api/queues/$S/nack" "{\"leaseToken\":\"$T\",\"error\":\"演示失败 #$i\"}" >/dev/null
  sleep 0.3  # 等待指数退避到期（演示启动时 base=200ms）
done

echo "== 5. 查看死信 =="
DEAD=$(req GET "/api/queues/dead-letters?topic=demo&consumerGroup=g-b")
echo "$DEAD"
DEAD_ID=$(echo "$DEAD" | field_num group_state_id)

echo "== 6. 手动重放死信 =="
req POST "/api/queues/dead-letters/$DEAD_ID/replay"; echo

echo "== 7. 再次消费并确认 =="
P=$(req POST /api/queues/poll '{"consumerGroup":"g-b","consumerId":"script-3"}')
echo "$P"
S=$(echo "$P" | field_num groupStateId)
T=$(echo "$P" | field_str leaseToken)
req POST "/api/queues/$S/ack" "{\"leaseToken\":\"$T\"}"; echo

echo "== 8. 数量统计与守恒 =="
req GET "/api/queues/stats?topic=demo"; echo
