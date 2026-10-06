package com.github.lechandonga.localmessagequeuejava.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 本地消息队列核心服务。
 *
 * <p>存储全部在嵌入式 H2 文件库中：消息内容 lmq_message、消费组与每组分发记录
 * lmq_delivery（即消费进度），去重依赖 lmq_message 的 (topic, dedup_key) 唯一约束。</p>
 */
@Service
public class MessageQueueService {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueService.class);

    private final JdbcTemplate jdbc;
    private final LmqProperties properties;

    /** 本进程实例标识：进程退出后，持锁的 IN_FLIGHT 消息会在停机/重启时被回收。 */
    private final String instanceId = UUID.randomUUID().toString();

    private static final String VIEW_SQL = """
            SELECT d.id AS delivery_id, d.message_id, d.topic, d.grp, d.status,
                   m.body, m.dedup_key, d.attempts, d.locked_by, d.available_at,
                   d.last_error
            FROM lmq_delivery d JOIN lmq_message m ON m.id = d.message_id
            """;

    private static final RowMapper<DeliveryView> VIEW_MAPPER = (rs, n) -> {
        DeliveryView v = new DeliveryView();
        v.setDeliveryId(rs.getLong("delivery_id"));
        v.setMessageId(rs.getLong("message_id"));
        v.setTopic(rs.getString("topic"));
        v.setGroup(rs.getString("grp"));
        v.setStatus(rs.getString("status"));
        v.setBody(rs.getString("body"));
        v.setDedupKey(rs.getString("dedup_key"));
        v.setAttempts(rs.getInt("attempts"));
        v.setLockedBy(rs.getString("locked_by"));
        v.setAvailableAt(rs.getLong("available_at"));
        v.setLastError(rs.getString("last_error"));
        return v;
    };

    public MessageQueueService(JdbcTemplate jdbc, LmqProperties properties) {
        this.jdbc = jdbc;
        this.properties = properties;
    }

    public String getInstanceId() {
        return instanceId;
    }

    /**
     * 注册消费组：组不存在则创建，并为历史消息补建该组的分发记录（每组各自独立消费）。
     */
    @Transactional
    public void registerGroup(String group) {
        int inserted = jdbc.update(
                "INSERT INTO lmq_group (name, created_at) VALUES (?, ?)",
                group, now());
        if (inserted > 0) {
            jdbc.update("""
                    INSERT INTO lmq_delivery (message_id, topic, grp, status, attempts,
                                              available_at, updated_at)
                    SELECT m.id, m.topic, ?, 'PENDING', 0, ?, ?
                    FROM lmq_message m
                    WHERE NOT EXISTS (
                        SELECT 1 FROM lmq_delivery d WHERE d.message_id = m.id AND d.grp = ?)
                    """, group, now(), now(), group);
            log.info("消费组[{}]已注册，历史消息回填完成", group);
        }
    }

    /**
     * 投递消息。dedupKey 为 null 表示不去重；相同 (topic, dedupKey) 重复投递幂等返回旧消息，
     * 不产生第二条待消费消息。
     *
     * @return 数组 [消息ID, 是否新入队]
     */
    @Transactional
    public Object[] publish(String topic, String body, String dedupKey) {
        long ts = now();
        long messageId;
        try {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbc.update(con -> {
                PreparedStatement ps = con.prepareStatement("""
                                INSERT INTO lmq_message (topic, body, dedup_key, created_at)
                                VALUES (?, ?, ?, ?)
                                """,
                        Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, topic);
                ps.setString(2, body);
                if (dedupKey == null) {
                    ps.setNull(3, java.sql.Types.VARCHAR);
                } else {
                    ps.setString(3, dedupKey);
                }
                ps.setLong(4, ts);
                return ps;
            }, keyHolder);
            Number key = keyHolder.getKey();
            if (key == null) {
                throw new IllegalStateException("消息插入未返回ID");
            }
            messageId = key.longValue();
        } catch (DuplicateKeyException e) {
            messageId = jdbc.queryForObject(
                    "SELECT id FROM lmq_message WHERE topic = ? AND dedup_key = ?",
                    Long.class, topic, dedupKey);
            log.info("投递去重生效 topic={} dedupKey={} -> 复用消息#{}，不产生新消息",
                    topic, dedupKey, messageId);
            return new Object[]{messageId, false};
        }

        // 为每个已注册消费组各建一条独立分发记录
        jdbc.update("""
                INSERT INTO lmq_delivery (message_id, topic, grp, status, attempts,
                                          available_at, updated_at)
                SELECT ?, m.topic, g.name, 'PENDING', 0, ?, ?
                FROM lmq_group g, lmq_message m
                WHERE m.id = ?
                """, messageId, ts, ts, messageId);
        log.info("消息入队 #{} topic={} dedupKey={}", messageId, topic, dedupKey);
        return new Object[]{messageId, true};
    }

    /**
     * 从指定消费组领取一条可投递消息（PENDING 或退避到期的 RETRYING）。
     * 行锁 + 单条条件 UPDATE 原子地置为 IN_FLIGHT 并加消费者租约，
     * 同组内同一条消息任意时刻只会被一个消费者处理。
     */
    @Transactional
    public DeliveryView poll(String group, String consumer) {
        registerGroupIfAbsent(group);
        Long candidate = jdbc.query("""
                        SELECT d.id FROM lmq_delivery d
                        WHERE d.grp = ?
                          AND d.status IN ('PENDING', 'RETRYING')
                          AND d.available_at <= ?
                        ORDER BY d.available_at, d.id
                        FETCH FIRST 1 ROWS ONLY
                        FOR UPDATE OF d
                        """,
                rs -> rs.next() ? rs.getLong(1) : null, group, now());
        if (candidate == null) {
            return null;
        }
        long ts = now();
        int locked = jdbc.update("""
                        UPDATE lmq_delivery
                        SET status = 'IN_FLIGHT', locked_by = ?, locked_at = ?, updated_at = ?
                        WHERE id = ? AND status IN ('PENDING', 'RETRYING')
                        """,
                consumer, ts, ts, candidate);
        if (locked == 0) {
            return null;
        }
        return getDelivery(candidate);
    }

    /** 确认成功。只有当前持锁消费者能确认；未成功绝不会被计入 SUCCEEDED。 */
    @Transactional
    public boolean ack(long deliveryId, String consumer) {
        int rows = jdbc.update("""
                        UPDATE lmq_delivery
                        SET status = 'SUCCEEDED', locked_by = NULL, locked_at = NULL,
                            updated_at = ?, last_error = NULL
                        WHERE id = ? AND status = 'IN_FLIGHT' AND locked_by = ?
                        """,
                now(), deliveryId, consumer);
        if (rows == 0) {
            log.warn("ack 失败：投递#{} 非处理中或消费者[{}]不持锁（租约可能已被回收）",
                    deliveryId, consumer);
            return false;
        }
        log.info("投递#{} 已确认成功 by [{}]", deliveryId, consumer);
        return true;
    }

    /**
     * 拒绝（处理失败）：attempts+1；未超上限进入 RETRYING 按指数退避延后，
     * 超过上限转入 DEAD 死信。只有当前持锁消费者能拒绝。
     */
    @Transactional
    public boolean reject(long deliveryId, String consumer, String error) {
        List<Integer> state = jdbc.queryForList("""
                        SELECT attempts FROM lmq_delivery
                        WHERE id = ? AND status = 'IN_FLIGHT' AND locked_by = ?
                        """,
                Integer.class, deliveryId, consumer);
        if (state.isEmpty()) {
            log.warn("reject 失败：投递#{} 非处理中或消费者[{}]不持锁", deliveryId, consumer);
            return false;
        }
        int attempts = state.get(0) + 1;
        long ts = now();
        String clippedError = error == null ? null
                : error.substring(0, Math.min(error.length(), 1900));
        if (attempts > properties.getMaxRetries()) {
            jdbc.update("""
                            UPDATE lmq_delivery
                            SET status = 'DEAD', attempts = ?, locked_by = NULL, locked_at = NULL,
                                updated_at = ?, last_error = ?
                            WHERE id = ?
                            """,
                    attempts, ts, clippedError, deliveryId);
            log.warn("投递#{} 重试次数 {} 超过上限 {}，进入死信",
                    deliveryId, attempts, properties.getMaxRetries());
        } else {
            long backoff = backoffMs(attempts);
            jdbc.update("""
                            UPDATE lmq_delivery
                            SET status = 'RETRYING', attempts = ?, locked_by = NULL,
                                locked_at = NULL, available_at = ?, updated_at = ?, last_error = ?
                            WHERE id = ?
                            """,
                    attempts, ts + backoff, ts, clippedError, deliveryId);
            log.info("投递#{} 被拒绝，第 {} 次重试，退避 {}ms", deliveryId, attempts, backoff);
        }
        return true;
    }

    /**
     * 死信手动重放：复用同一条分发记录回到 RETRYING，退避后再投递。
     * 不重置 attempts、不新建消息，因此绕不过重试上限与去重规则；
     * 下一次再失败会立即再次进入死信。
     */
    @Transactional
 public boolean replayDead(long deliveryId) {
        Integer attempts = jdbc.queryForObject(
                "SELECT attempts FROM lmq_delivery WHERE id = ?", Integer.class, deliveryId);
        if (attempts == null) {
            log.warn("死信重放失败：投递#{} 不存在", deliveryId);
            return false;
        }
        long ts = now();
        long backoff = backoffMs(Math.max(1, attempts));
        int rows = jdbc.update("""
                        UPDATE lmq_delivery
                        SET status = 'RETRYING', locked_by = NULL, locked_at = NULL,
                            available_at = ?, updated_at = ?
                        WHERE id = ? AND status = 'DEAD'
                        """,
                ts + backoff, ts, deliveryId);
        if (rows == 0) {
            log.warn("死信重放失败：投递#{} 当前不是 DEAD 状态", deliveryId);
            return false;
        }
        log.info("死信投递#{} 手动重放，保留 attempts={}，{}ms 后可再次投递",
                deliveryId, attempts, backoff);
        return true;
    }

    /** 查看某组的死信列表。 */
    public List<DeliveryView> listDead(String group) {
        return jdbc.query(VIEW_SQL + " WHERE d.grp = ? AND d.status = 'DEAD' "
                + "ORDER BY d.updated_at", VIEW_MAPPER, group);
    }

    /** 单条分发记录详情。 */
    public DeliveryView getDelivery(long deliveryId) {
        List<DeliveryView> list = jdbc.query(VIEW_SQL + " WHERE d.id = ?",
                VIEW_MAPPER, deliveryId);
        return list.isEmpty() ? null : list.get(0);
    }

    /**
     * 按组统计。pending + inFlight + retrying + succeeded + dead = 该组投递总量。
     */
    public Map<String, Object> stats(String group) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("group", group);
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT status, COUNT(*) AS cnt FROM lmq_delivery WHERE grp = ? GROUP BY status
                """, group);
        long total = 0;
        for (DeliveryStatus s : DeliveryStatus.values()) {
            long cnt = rows.stream()
                    .filter(r -> s.name().equals(r.get("STATUS")))
                    .mapToLong(r -> ((Number) r.get("CNT")).longValue())
                    .findFirst().orElse(0L);
            result.put(label(s), cnt);
            total += cnt;
        }
        result.put("total", total);
        return result;
    }

    /**
     * 租约回收：超过 leaseMs 仍未确认的 IN_FLIGHT 消息（消费者异常/崩溃/进程退出）
     * 退回 PENDING 立即重新投递。不增加 attempts —— 崩溃不算一次业务失败。
     */
    @Transactional
    public int reclaimExpiredLocks() {
        long deadline = now() - properties.getLeaseMs();
        int rows = jdbc.update("""
                        UPDATE lmq_delivery
                        SET status = 'PENDING', locked_by = NULL, locked_at = NULL,
                            available_at = ?, updated_at = ?
                        WHERE status = 'IN_FLIGHT' AND locked_at < ?
                        """,
                now(), now(), deadline);
        if (rows > 0) {
            log.warn("回收 {} 条租约超时（消费者异常/进程退出）的处理中消息，重回待投递", rows);
        }
        return rows;
    }

    /** 停机处理：所有 IN_FLIGHT 消息立即释放为可投递，保证重启后未确认消息不丢。 */
    @Transactional
    public int releaseOnShutdown() {
        int rows = jdbc.update("""
                        UPDATE lmq_delivery
                        SET status = 'PENDING', locked_by = NULL, locked_at = NULL,
                            available_at = ?, updated_at = ?
                        WHERE status = 'IN_FLIGHT'
                        """,
                now(), now());
        if (rows > 0) {
            log.info("停机释放 {} 条处理中消息为可投递状态", rows);
        }
        return rows;
    }

    private long backoffMs(int failedAttempt) {
        double raw = properties.getBackoff().getBaseMs()
                * Math.pow(properties.getBackoff().getMultiplier(), failedAttempt - 1);
        return Math.min(properties.getBackoff().getCapMs(), (long) raw);
    }

    private void registerGroupIfAbsent(String group) {
        jdbc.update("INSERT INTO lmq_group (name, created_at) "
                        + "SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM lmq_group WHERE name = ?)",
                group, now(), group);
    }

    private String label(DeliveryStatus s) {
        return switch (s) {
            case PENDING -> "pending";
            case IN_FLIGHT -> "inFlight";
            case RETRYING -> "retrying";
            case SUCCEEDED -> "succeeded";
            case DEAD -> "dead";
        };
    }

    private long now() {
        return System.currentTimeMillis();
    }
}
