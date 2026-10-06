#!/usr/bin/env bash
# 端到端演示：注册组 -> 投递(含去重) -> 消费 -> 重试进死信 -> 重放 -> 状态守恒检查
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"; Q="$DIR/lmq.sh"
G="demo-group"; T="demo-topic"
echo "== 注册消费组 =="; "$Q" groups "$G"
echo "== 首次投递 =="; "$Q" publish "$T" "hello" "k-1"
echo "== 重复投递(应 created=false, deduplicated=true, 同一 messageId) =="; "$Q" publish "$T" "hello-again" "k-1"
echo "== 领取并确认 =="; D=$("$Q" poll "$G" "worker-1")
echo "$D"
ID=$(echo "$D" | python3 -c 'import sys,json;print(json.load(sys.stdin)["deliveryId"])')
"$Q" ack "$ID" "worker-1" >/dev/null && echo "acked $ID"
echo "== 第二条消息演示失败重试直到死信(maxRetries=3) =="
"$Q" publish "$T" "flaky" >/dev/null
for i in 1 2 3 4; do
  D=$("$Q" poll "$G" "worker-2")
  ID=$(echo "$D" | python3 -c 'import sys,json;print(json.load(sys.stdin)["deliveryId"])')
  echo "-- attempt $i, deliveryId=$ID"
  "$Q" reject "$ID" "worker-2" "simulated failure $i" >/dev/null
  "$Q" status "$ID" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("   status=",d["status"],"attempts=",d["attempts"])'
  sleep 1.2
done
echo "== 死信列表 =="; "$Q" dead "$G"
echo "== 手动重放（保留 attempts，退避后可再领，再失败仍回死信） =="
DEAD_ID=$("$Q" dead "$G" | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["deliveryId"])')
"$Q" replay "$DEAD_ID"
sleep 1.2
echo "== 状态守恒检查: pending+inFlight+retrying+succeeded+dead == total =="; "$Q" stats "$G"
