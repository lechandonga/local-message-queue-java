# 本地消息队列使用说明

一个不依赖任何外部中间件的本地消息队列：消息内容、去重记录、消费进度全部落在
进程本地的嵌入式 H2 文件库（默认目录 `./data/lmq/`）。只需一个 Spring Boot 进程，
通过 HTTP/JSON 接口即可完成投递、消费、确认、拒绝、死信重放与状态查询。

## 快速开始

```bash
mvn spring-boot:run            # 默认 8080 端口，数据落在 ./data/lmq/
# 或
mvn package && java -jar target/local-message-queue-java-1.0-SNAPSHOT.jar
```

用自带脚本复现（脚本在 `scripts/lmq.sh`，端口可用环境变量 `LMQ_BASE` 覆盖）：

```bash
export LMQ_BASE=http://localhost:8080/api/queue
scripts/lmq.sh groups order-group                       # 注册消费组
scripts/lmq.sh publish orders '{"id":1}' order-1001     # 首次投递 -> created=true
scripts/lmq.sh publish orders '{"id":1}' order-1001     # 重复投递 -> created=false（幂等）
scripts/lmq.sh poll order-group worker-1                # 领取（带租约）
scripts/lmq.sh ack <deliveryId> worker-1                # 确认成功
scripts/lmq.sh reject <deliveryId> worker-1 '原因'      # 拒绝（退避重试或进死信）
scripts/lmq.sh status <deliveryId>                      # 查单条投递状态
scripts/lmq.sh dead order-group                         # 查看死信
scripts/lmq.sh replay <deliveryId>                      # 手动重放死信
scripts/lmq.sh stats order-group                        # 五类状态数量 + total（校验守恒）
scripts/lmq.sh config                                   # 查生效配置
```

`scripts/demo.sh` 串起了注册、去重、消费、重试进死信、重放、守恒检查的完整流程。

> 注意：应先注册消费组再投递消息，组才能收到投递时刻的全部消息；之后再注册的组
> 只会收到注册之后的新消息（注册时会回填当时已有的历史消息）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/queue/groups/{group}` | 注册消费组（幂等） |
| POST | `/api/queue/topics/{topic}/messages` | 投递，请求体 `{"body":"...","dedupKey":"可选"}`，返回 `created` 与 `deduplicated` |
| POST | `/api/queue/groups/{group}/poll?consumer=xxx` | 领取一条可投递消息，无消息返回 `null` |
| POST | `/api/queue/deliveries/{id}/ack?consumer=xxx` | 确认成功 |
| POST | `/api/queue/deliveries/{id}/reject?consumer=xxx` | 拒绝，请求体 `{"error":"..."}`，触发重试/死信 |
| GET  | `/api/queue/deliveries/{id}` | 查询单条投递状态 |
| GET  | `/api/queue/groups/{group}/dead` | 查看该组死信列表 |
| POST | `/api/queue/deliveries/{id}/replay` | 手动重放死信 |
| GET  | `/api/queue/groups/{group}/stats` | 状态数量统计（含 `total`） |
| GET  | `/api/queue/config` | 当前生效配置与实例ID |

## 投递语义

- **按主题投递**：消息属于 `topic`，消费者按消费组（group）独立消费。
- **多消费组独立**：每条消息在每个注册组各有一条独立的分发记录（`lmq_delivery`），
  组之间进度互不影响，天然支持发布/订阅式的多方消费。
- **同组单消费者**：领取（poll）在数据库事务内通过 `SELECT ... FOR UPDATE` 选行 +
  条件 `UPDATE ... SET status='IN_FLIGHT'` 原子加锁。同组内一条消息任意时刻只会被
  一个消费者处理；并发消费者中抢不到锁的一方拿不到该消息。
- **租约模型**：领取后消息进入 `IN_FLIGHT` 并记录 `locked_by` / `locked_at`。
  - 正常处理完成后调用 `ack` 置为 `SUCCEEDED`；只有当前持锁消费者能确认。
  - 业务失败调用 `reject`；消费者代码抛出异常、忘记确认、进程退出时消息不会丢失，
    也绝不会被计入成功，而是在租约超时后由后台任务回收，或停机/重启时回收，
    重新变为 `PENDING` 可投递。
- 状态集合：`PENDING`（待投递）、`IN_FLIGHT`（处理中）、`RETRYING`（退避等待重试）、
  `SUCCEEDED`（成功）、`DEAD`（死信）。
- **数量守恒**：对任一组，`pending + inFlight + retrying + succeeded + dead`
  恒等于该组的投递总量，可通过 `stats` 接口的 `total` 字段直接校验。

## 重试与死信规则

- 每次 `reject` 使 `attempts + 1`；租约超时/进程崩溃导致的回收 **不增加** `attempts`
  （崩溃不算一次业务失败）。
- `attempts <= maxRetries`：进入 `RETRYING`，按指数退避延后，到期后自动重新可投递。
  第 n 次失败的退避时间为 `min(backoff-cap-ms, backoff-base-ms * multiplier^(n-1))`。
- `attempts > maxRetries`：进入 `DEAD` 死信，不再自动投递，可在
  `GET /groups/{group}/dead` 单独查看。
- 死信可调用 `replay` 手动重放：**复用同一条分发记录**、保留已有 `attempts`，
  进入 `RETRYING` 退避后重新投递。因此重放绕不过重试上限——再失败一次会立即
  再次进入死信；重放也不会新建消息，天然不绕过去重规则。

## 去重规则

- 去重键作用域为 `(topic, dedupKey)`，由消息表的唯一约束保证，并发投递也安全。
- 相同 `(topic, dedupKey)` 的重复投递：复用已有消息ID，响应
  `created=false / deduplicated=true`，**不会**产生第二条消息或第二条待消费记录；
  后到投递的 body 不会覆盖先到内容。
- `dedupKey` 为空（null）表示不去重，每次投递都是新消息。
- 调用方通过响应中的 `created` 字段即可区分“新入队”与“被去重”。

## 停机与重启恢复

- 正常停机（SIGTERM / Spring 优雅关闭）：收到关闭事件后，所有 `IN_FLIGHT` 消息
  立即释放为 `PENDING`，重启后可继续消费。
- 异常退出（kill -9、宕机）：消息仍处于 `IN_FLIGHT`，重启时与后台扫描任务会把
  超过 `lease-ms` 的处理中消息回收为 `PENDING`。消息、进度都已在每次操作中落盘，
  重启后未确认消息不丢。
- JDBC URL 中的 `DB_CLOSE_ON_EXIT=FALSE` 用于保证 H2 不会在 Spring 停机流程之前
  提前关闭数据库。

## 可配置项（`application.properties`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `server.port` | 8080 | HTTP 端口 |
| `spring.datasource.url` | `jdbc:h2:file:./data/lmq/queue;...` | 本地 H2 文件库位置，删除目录即可清空队列 |
| `lmq.max-retries` | 3 | 最大重试次数，超过后进死信（0 表示首次失败即死信） |
| `lmq.backoff.base-ms` | 1000 | 指数退避基数（毫秒） |
| `lmq.backoff.multiplier` | 2.0 | 退避倍数 |
| `lmq.backoff.cap-ms` | 30000 | 单次退避上限（毫秒） |
| `lmq.lease-ms` | 30000 | 处理中租约：超过该时长未确认即回收重投 |
| `lmq.sweep-interval-ms` | 1000 | 后台租约回收扫描间隔 |

所有配置均可通过 JVM 参数覆盖，例如：
`java -jar app.jar --lmq.max-retries=5 --lmq.backoff.base-ms=2000`。

## 本地验证

```bash
mvn test        # 6 个测试全部通过，日志中以 [场景N] 标注每个业务用例的步骤与结果
```

测试覆盖：重复投递幂等去重、重试耗尽进死信、死信重放不绕过上限、
多生产者/多消费者并发（数量守恒 + 不会同时处理同一条）、停机释放与重启恢复。
