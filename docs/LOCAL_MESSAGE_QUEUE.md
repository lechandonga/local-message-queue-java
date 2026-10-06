# 本地消息队列（Local Message Queue）

纯本地实现的轻量级消息队列：消息内容、去重记录、消费进度全部落在本地嵌入式
H2 文件数据库中，**不需要部署任何外部中间件**（无需 Kafka/RabbitMQ/独立数据库）。

## 1. 快速开始

```bash
# 构建 + 跑全部测试
mvn clean test

# 启动服务（默认端口 8080，数据落在 ./data/）
mvn spring-boot:run

# 用脚本走一遍完整业务（投递/去重/消费/确认/重试/死信/重放/统计）
./scripts/demo.sh
```

## 2. 数据模型（本地持久化）

| 表 | 作用 |
| --- | --- |
| `mq_message` | 消息内容。`(topic, dedup_key)` 唯一约束承载去重记录；不带去重键的消息入库时写入随机 UUID，互不影响 |
| `mq_group_state` | 每个消费者组对每条消息的消费进度：状态、投递次数、退避可见时间、租约、持有者、最近错误 |

进度行采用“懒扇出”：消息投递后不立即为所有组建行，某消费者组第一次 poll
时才把主题下缺失的进度补齐为 `PENDING`。因此多个消费者组各自独立消费同一份消息，
互不影响。

## 3. HTTP 入口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/queues/messages` | 投递消息，body：`{"topic","dedupKey","payload"}`；返回 `created`（true 新入队 / false 命中去重）与 `messageId` |
| `POST /api/queues/poll` | 拉取消息，body：`{"consumerGroup","consumerId"}`；204 表示暂无消息；返回租约 `leaseToken` 与 `groupStateId` |
| `POST /api/queues/{groupStateId}/ack` | 确认成功，body：`{"leaseToken"}`；租约失效返回 409 |
| `POST /api/queues/{groupStateId}/nack` | 拒绝/失败，body：`{"leaseToken","error"}`；触发退避重试或进入死信 |
| `GET  /api/queues/messages/{messageId}` | 查询消息内容及各消费者组状态 |
| `GET  /api/queues/dead-letters?topic=&consumerGroup=` | 查看死信列表 |
| `POST /api/queues/dead-letters/{groupStateId}/replay` | 手动重放死信 |
| `GET  /api/queues/stats?topic=&consumerGroup=` | 数量统计与守恒校验 `conserved` |

### 典型流程

```bash
curl -s -X POST localhost:8080/api/queues/messages -H 'Content-Type: application/json' \
  -d '{"topic":"order","dedupKey":"o-1","payload":"hello"}'

curl -s -X POST localhost:8080/api/queues/poll -H 'Content-Type: application/json' \
  -d '{"consumerGroup":"g-a","consumerId":"c1"}'

curl -s -X POST localhost:8080/api/queues/1/ack  -H 'Content-Type: application/json' \
  -d '{"leaseToken":"<poll 返回的 token>"}'
```

## 4. 投递语义

- **按主题（topic）投递**，消息按 `visible_at, id` 先进先出派发。
- **消费者组独立**：不同组各自拥有一份消费进度，同一条消息每组都会消费一遍。
- **组内互斥（至少一次投递）**：派发时在事务内 `SELECT ... FOR UPDATE`
  锁定进度行并立即置为 `PROCESSING`、写入租约 token。同一时刻同一条消息只会被
  组内一个消费者拿到。
- **必须显式确认**：消费者拿到消息后只能 `ack`（成功）或 `nack`（失败）。
  租约 token 不匹配的 ack/nack 一律拒绝（HTTP 409），避免迟到确认把已重投的
  消息误计为成功。
- 语义为 **at-least-once**：崩溃重投后消费者侧可能重复处理，业务方可用
  `dedupKey`/messageId 做幂等。

## 5. 去重规则

- 去重键由调用方提供，作用域为 `(topic, dedupKey)`；不同 topic 同名键互不影响。
- 相同 `(topic, dedupKey)` 的重复投递**幂等**：只保留第一条消息与内容，
  返回 `created=false`（`deduped=true`）供调用方区分，**不会产生第二条待消费消息**。
- 不传 `dedupKey`（或空串）表示不去重，每条消息独立入队。

## 6. 重试与退避

- 每次 `nack` 后消息回到 `PENDING`，并设置指数退避可见时间：
  `delay = min(retryBackoffBaseMs * 2^(attempt-1), retryBackoffMaxMs)`。
- 投递次数 `attempts` 在每次 poll 派发时加 1；当 nack 时 `attempts > maxRetries`
  （默认 3，即最多投递 4 次），消息进入 `DEAD` 死信状态，`last_error` 记录失败原因。
- **租约超时回收不计重试次数**：消费者崩溃/处理中异常/进程退出导致未确认的消息，
  超过 `leaseMs` 后被后台回收器直接放回可投递队列。

## 7. 死信与重放

- 死信可按 topic / consumerGroup 单独查看，保留原始 payload 与错误信息。
- 手动 replay 把该组进度重置为 `PENDING`、`attempts=0` 后重新排队：
  - **复用原消息**，不产生新消息，因此不绕过去重规则；
  - 重放后仍受同一个 `maxRetries` 上限约束，再次失败耗尽仍会回到死信。

## 8. 停机、崩溃与重启恢复

消息不会被静默丢弃，也不会在未成功时被计入成功，三重保障：

1. **租约超时**：`PROCESSING` 超过 `lq.lease-ms` 未确认，后台回收器（每
   `lq.reap-interval-ms` 扫描一次）自动放回 `PENDING`。
2. **优雅停机**：收到 Spring 停机信号（SIGTERM / `context.close()`）时，
   `@PreDestroy` 先停回收器，再把所有 `PROCESSING` 立即释放为 `PENDING`。
3. **启动恢复**：重启后把上次进程遗留的 `PROCESSING` 行（例如被 `kill -9`）
   全部恢复为 `PENDING`。

## 9. 数量守恒

`GET /api/queues/stats` 返回 `total / succeeded / processing / pending / retrying / dead`
与 `conserved` 标志：

- 指定 `consumerGroup` 时：成功 + 处理中 + 待投递 + 重试退避中 + 死信
  （含尚未扇出到该组的消息）= 投递总量。
- 不指定组时按消息维度聚合（一条消息在多个组的进度取最靠前的业务状态），
  五者之和同样等于投递总量。

## 10. 可配置项（`application.properties` 前缀 `lq`）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `lq.data-dir` | `./data` | H2 数据文件目录，所有本地状态存放处 |
| `lq.lease-ms` | `30000` | 消费租约时长，超时未确认自动回收 |
| `lq.reap-interval-ms` | `1000` | 租约回收扫描间隔 |
| `lq.max-retries` | `3` | 最大重试次数，第 `maxRetries+1` 次投递仍失败则进死信 |
| `lq.retry-backoff-base-ms` | `1000` | 指数退避初始值 |
| `lq.retry-backoff-max-ms` | `60000` | 退避上限 |
| `lq.default-visible-delay-ms` | `0` | 新消息首次可见延迟 |

## 11. 测试覆盖

`mvn clean test` 会输出带 `[用例开始]/[用例通过]` 的业务日志：

| 测试类 | 覆盖业务 |
| --- | --- |
| `DedupPublishTest` | 重复投递幂等、created 区分、不产生第二条消息、无 dedupKey 不去重 |
| `RetryDeadLetterTest` | 指数退避重试、重试耗尽进死信、死信查看、手动重放不绕过去重与上限 |
| `ConcurrentConsumeTest` | 4 生产者 + 12 消费者并发、两个消费组独立、同一时刻无重复处理、数量守恒 |
| `ShutdownRecoveryTest` | 租约超时回收、旧租约迟到 ack 被拒、优雅停机释放、重启后未确认消息恢复不丢 |
