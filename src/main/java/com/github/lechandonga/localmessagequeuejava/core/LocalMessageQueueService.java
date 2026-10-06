package com.github.lechandonga.localmessagequeuejava.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.lechandonga.localmessagequeuejava.config.LocalQueueProperties;

/**
 * 本地消息队列核心服务。
 *
 * <p>消息内容存 mq_message（topic+dedupKey 唯一），每个消费者组的消费进度存
 * mq_group_state。派发时用 SELECT ... FOR UPDATE 行锁保证同一组内一条消息同一时刻
 * 只会被一个消费者拿到。</p>
 */
@Service
public class LocalMessageQueueService {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final LocalQueueProperties props;

    public LocalMessageQueueService(JdbcTemplate jdbc,
                                    PlatformTransactionManager txManager,
                                    LocalQueueProperties props) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(txManager);
        this.props = props;
    }

    public record PublishResult(boolean created, Long messageId) {
    }

    /**
     * 按主题投递消息。dedupKey 非空时做幂等去重：同一 (topic, dedupKey) 只保留第一条，
     * 重复投递返回 created=false，且不会产生第二条待消费消息。
     */
    public PublishResult publish(String topic, String dedupKey, String payload) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload 不能为空");
        }
        long now = System.currentTimeMillis();
        long visibleAt = now + Math.max(0, props.getDefaultVisibleDelayMs());
        boolean provided = dedupKey != null && !dedupKey.isEmpty();
        String storedKey = provided ? dedupKey : UUID.randomUUID().toString();
        try {
            Long id = tx.execute(s -> {
                jdbc.update("INSERT INTO mq_message(topic, dedup_key, dedup_key_provided, "
                        + "payload, created_at) VALUES(?,?,?,?,?)",
                        topic, storedKey, provided, payload, visibleAt);
                if (!provided) {
                    return jdbc.queryForObject("CALL IDENTITY()", Long.class);
                }
                return jdbc.queryForObject(
                        "SELECT id FROM mq_message WHERE topic=? AND dedup_key=?",
                        Long.class, topic, storedKey);
            });
            return new PublishResult(true, id);
        } catch (DuplicateKeyException e) {
            Long existing = jdbc.queryForObject(
                    "SELECT id FROM mq_message WHERE topic=? AND dedup_key=?",
                    Long.class, topic, storedKey);
            return new PublishResult(false, existing);
        }
    }

    /**
     * 从指定消费者组拉取一条可消费消息；没有可消费消息时返回 null。
     * 拿到消息后必须在租约内 ack 或 nack。
     */
    public PolledMessage poll(String consumerGroup, String consumerId) {
        if (consumerGroup == null || consumerGroup.isBlank()) {
            throw new IllegalArgumentException("consumerGroup 不能为空");
        }
        String cid = (consumerId == null || consumerId.isBlank()) ? "default" : consumerId;
        return tx.execute(status -> doPoll(consumerGroup, cid));
    }

    private PolledMessage doPoll(String group, String consumerId) {
        long now = System.currentTimeMillis();
        // 懒扇出：为所有还没有该组进度的消息补 PENDING 进度行。
        try {
            jdbc.update("""
                    INSERT INTO mq_group_state
                        (message_id, topic, consumer_group, status, attempts, visible_at, updated_at)
                    SELECT m.id, m.topic, ?, 'PENDING', 0, m.created_at, ?
                    FROM mq_message m
                    WHERE NOT EXISTS (
                        SELECT 1 FROM mq_group_state g
                        WHERE g.message_id = m.id AND g.consumer_group = ?)
                    """, group, now, group);
        } catch (DuplicateKeyException ignored) {
            // 与并发消费者同时扇出，唯一约束输掉的一方直接复用已存在的行。
        }

        Long stateId = jdbc.query("""
                SELECT id FROM mq_group_state
                WHERE consumer_group = ? AND status = 'PENDING' AND visible_at <= ?
                ORDER BY visible_at, id
                LIMIT 1
                FOR UPDATE
                """, rs -> rs.next() ? rs.getLong(1) : null, group, now);
        if (stateId == null) {
            return null;
        }
        String token = UUID.randomUUID().toString();
        long expires = now + props.getLeaseMs();
        jdbc.update("""
                UPDATE mq_group_state
                SET status = 'PROCESSING', attempts = attempts + 1,
                    lease_expires_at = ?, lease_token = ?, consumer_id = ?, updated_at = ?
                WHERE id = ?
                """, expires, token, consumerId, now, stateId);

        Map<String, Object> row = jdbc.queryForMap("""
                SELECT g.message_id, g.topic, g.attempts, m.payload
                FROM mq_group_state g JOIN mq_message m ON m.id = g.message_id
                WHERE g.id = ?
                """, stateId);
        return new PolledMessage(
                stateId,
                ((Number) row.get("message_id")).longValue(),
                (String) row.get("topic"),
                group,
                consumerId,
                (String) row.get("payload"),
                ((Number) row.get("attempts")).intValue(),
                token,
                expires);
    }

    /** 确认消费成功；租约不匹配（已过期被别人拿走）时返回 false。 */
    public boolean ack(long groupStateId, String leaseToken) {
        long now = System.currentTimeMillis();
        Integer updated = tx.execute(s -> jdbc.update("""
                UPDATE mq_group_state
                SET status = 'SUCCEEDED', lease_expires_at = 0, lease_token = NULL, updated_at = ?
                WHERE id = ? AND status = 'PROCESSING' AND lease_token = ?
                """, now, groupStateId, leaseToken));
        return updated != null && updated > 0;
    }

    /**
     * 拒绝（消费失败）。未超重试上限则按指数退避重新排队；超过上限转入死信。
     * 租约不匹配返回 false，消息不会被误判成功，也不会被错误重试。
     */
    public boolean nack(long groupStateId, String leaseToken, String error) {
        Boolean result = tx.execute(s -> {
            long now = System.currentTimeMillis();
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT status, lease_token, attempts FROM mq_group_state "
                            + "WHERE id = ? FOR UPDATE", groupStateId);
            if (rows.isEmpty()) {
                return false;
            }
            Map<String, Object> row = rows.get(0);
            if (!"PROCESSING".equals(row.get("status"))
                    || !Objects.equals(leaseToken, row.get("lease_token"))) {
                return false;
            }
            int attempts = ((Number) row.get("attempts")).intValue();
            if (attempts > props.getMaxRetries()) {
                jdbc.update("""
                        UPDATE mq_group_state
                        SET status = 'DEAD', lease_expires_at = 0, lease_token = NULL,
                            last_error = ?, updated_at = ?
                        WHERE id = ?
                        """, trimError(error), now, groupStateId);
            } else {
                jdbc.update("""
                        UPDATE mq_group_state
                        SET status = 'PENDING', visible_at = ?, lease_expires_at = 0,
                            lease_token = NULL, last_error = ?, updated_at = ?
                        WHERE id = ?
                        """, now + backoffMillis(attempts), trimError(error), now, groupStateId);
            }
            return true;
        });
        return Boolean.TRUE.equals(result);
    }

    private long backoffMillis(int attempt) {
        long base = Math.max(1, props.getRetryBackoffBaseMs());
        long delay = base;
        for (int i = 1; i < attempt; i++) {
            if (delay >= props.getRetryBackoffMaxMs() / 2) {
                return props.getRetryBackoffMaxMs();
            }
            delay *= 2;
        }
        return Math.min(delay, props.getRetryBackoffMaxMs());
    }

    private String trimError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 4000 ? error.substring(0, 4000) : error;
    }

    /**
     * 回收租约超时的消息（消费者崩溃 / 处理中异常未 nack / 进程退出），
     * 不计重试次数，直接回到可投递状态。
     *
     * @return 本次回收条数
     */
    public int reapExpiredLeases() {
        long now = System.currentTimeMillis();
        return jdbc.update("""
                UPDATE mq_group_state
                SET status = 'PENDING', lease_expires_at = 0, lease_token = NULL, updated_at = ?
                WHERE status = 'PROCESSING' AND lease_expires_at <= ?
                """, now, now);
    }

    /** 启动恢复：上次进程遗留的 PROCESSING 消息全部恢复为可投递。 */
    public int recoverOrphanedProcessing() {
        long now = System.currentTimeMillis();
        return jdbc.update("""
                UPDATE mq_group_state
                SET status = 'PENDING', lease_expires_at = 0, lease_token = NULL, updated_at = ?
                WHERE status = 'PROCESSING'
                """, now);
    }

    /** 停机保护：收到停机信号时正在处理的消息立刻回到可投递状态。 */
    public int releaseProcessingOnShutdown() {
        long now = System.currentTimeMillis();
        return jdbc.update("""
                UPDATE mq_group_state
                SET status = 'PENDING', lease_expires_at = 0, lease_token = NULL, updated_at = ?
                WHERE status = 'PROCESSING'
                """, now);
    }

    /** 查看死信列表（可按主题/组过滤）。 */
    public List<Map<String, Object>> deadLetters(String topic, String consumerGroup) {
        StringBuilder sql = new StringBuilder("""
                SELECT g.id AS group_state_id, g.message_id, g.topic, g.consumer_group,
                       g.attempts, g.last_error, g.updated_at, m.payload
                FROM mq_group_state g JOIN mq_message m ON m.id = g.message_id
                WHERE g.status = 'DEAD'
                """);
        List<Object> args = new ArrayList<>();
        if (topic != null && !topic.isBlank()) {
            sql.append(" AND g.topic = ?");
            args.add(topic);
        }
        if (consumerGroup != null && !consumerGroup.isBlank()) {
            sql.append(" AND g.consumer_group = ?");
            args.add(consumerGroup);
        }
        sql.append(" ORDER BY g.id");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    /**
     * 手动重放死信：复用原消息（不产生新消息、不绕过去重），attempts 清零重新排队；
     * 重放后的消息仍受同样的重试上限约束，再次耗尽仍会进死信。
     */
    public boolean replayDeadLetter(long groupStateId) {
        long now = System.currentTimeMillis();
        Integer updated = tx.execute(s -> jdbc.update("""
                UPDATE mq_group_state
                SET status = 'PENDING', attempts = 0, visible_at = ?,
                    lease_expires_at = 0, lease_token = NULL, consumer_id = NULL, updated_at = ?
                WHERE id = ? AND status = 'DEAD'
                """, now, now, groupStateId));
        return updated != null && updated > 0;
    }

    /** 查询单条消息内容及各消费者组状态。 */
    public Map<String, Object> messageStatus(long messageId) {
        List<Map<String, Object>> msgs = jdbc.queryForList(
                "SELECT id, topic, CASE WHEN dedup_key_provided THEN dedup_key ELSE NULL END "
                        + "AS dedup_key, payload, created_at FROM mq_message WHERE id = ?",
                messageId);
        if (msgs.isEmpty()) {
            return null;
        }
        Map<String, Object> result = new LinkedHashMap<>(msgs.get(0));
        result.put("groups", jdbc.queryForList("""
                SELECT consumer_group, status, attempts, visible_at, lease_expires_at,
                       consumer_id, last_error, updated_at
                FROM mq_group_state WHERE message_id = ? ORDER BY consumer_group
                """, messageId));
        return result;
    }

    /**
     * 数量统计与守恒校验：成功 + 处理中 + 待投递 + 重试退避中 + 死信
     * （含尚未扇出的组进度）等于投递总量。
     */
    public Map<String, Object> stats(String topic, String consumerGroup) {
        long now = System.currentTimeMillis();

        StringBuilder totalSql = new StringBuilder("SELECT COUNT(*) FROM mq_message");
        List<Object> totalArgs = new ArrayList<>();
        if (topic != null && !topic.isBlank()) {
            totalSql.append(" WHERE topic = ?");
            totalArgs.add(topic);
        }
        long total = jdbc.queryForObject(totalSql.toString(), Long.class, totalArgs.toArray());

        StringBuilder stateSql = new StringBuilder("""
                SELECT
                  SUM(CASE WHEN g.status = 'SUCCEEDED' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN g.status = 'PROCESSING' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN g.status = 'DEAD' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN g.status = 'PENDING' AND g.visible_at <= ? THEN 1 ELSE 0 END),
                  SUM(CASE WHEN g.status = 'PENDING' AND g.visible_at > ? THEN 1 ELSE 0 END),
                  COUNT(*)
                FROM mq_group_state g
                WHERE 1=1
                """);
        List<Object> stateArgs = new ArrayList<>(List.of(now, now));
        if (topic != null && !topic.isBlank()) {
            stateSql.append(" AND g.topic = ?");
            stateArgs.add(topic);
        }
        if (consumerGroup != null && !consumerGroup.isBlank()) {
            stateSql.append(" AND g.consumer_group = ?");
            stateArgs.add(consumerGroup);
        }
        List<Long> counts = jdbc.query(stateSql.toString(), rs -> {
            rs.next();
            return List.of(
                    rs.getLong(1), rs.getLong(2), rs.getLong(3),
                    rs.getLong(4), rs.getLong(5), rs.getLong(6));
        }, stateArgs.toArray());

        long succeeded = counts.get(0);
        long processing = counts.get(1);
        long dead = counts.get(2);
        long ready = counts.get(3);
        long retrying = counts.get(4);
        long tracked = counts.get(5);

        // 未扇出（从未被任何 poll 过）的消息视为“待投递”。
        boolean groupSpecified = consumerGroup != null && !consumerGroup.isBlank();
        long unfanned;
        long accounted;
        if (groupSpecified) {
            unfanned = total - tracked;
            accounted = succeeded + processing + dead + ready + retrying + unfanned;
        } else {
            // 未指定组时一条消息在多个组各有进度，按消息维度聚合状态后再守恒：
            // 取该消息在所有组中“最靠前”的业务状态（成功 > 死信 > 重试中 > 处理中 > 待投递）。
            List<Object> aggArgs = new ArrayList<>();
            StringBuilder aggSql = new StringBuilder("""
                    SELECT
                      SUM(CASE WHEN best = 4 THEN 1 ELSE 0 END),
                      SUM(CASE WHEN best = 3 THEN 1 ELSE 0 END),
                      SUM(CASE WHEN best = 2 THEN 1 ELSE 0 END),
                      SUM(CASE WHEN best = 1 THEN 1 ELSE 0 END),
                      COUNT(DISTINCT message_id)
                    FROM (
                      SELECT message_id,
                        MAX(CASE
                          WHEN status = 'SUCCEEDED' THEN 4
                          WHEN status = 'DEAD' THEN 3
                          WHEN status = 'PENDING' AND visible_at > ? THEN 2
                          WHEN status = 'PROCESSING' THEN 1
                          ELSE 0 END) AS best
                      FROM mq_group_state
                      WHERE 1=1
                    """);
            aggArgs.add(now);
            if (topic != null && !topic.isBlank()) {
                aggSql.append(" AND topic = ?");
                aggArgs.add(topic);
            }
            aggSql.append(" GROUP BY message_id)");
            List<Long> agg = jdbc.query(aggSql.toString(), rs -> {
                rs.next();
                return List.of(rs.getLong(1), rs.getLong(2), rs.getLong(3),
                        rs.getLong(4), rs.getLong(5));
            }, aggArgs.toArray());
            succeeded = agg.get(0);
            dead = agg.get(1);
            retrying = agg.get(2);
            processing = agg.get(3);
            long distinctTracked = agg.get(4);
            unfanned = total - distinctTracked;
            ready = 0L;
            tracked = distinctTracked;
            accounted = succeeded + processing + dead + retrying + unfanned;
        }
        long pendingReady = ready + unfanned;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topic", topic);
        result.put("consumerGroup", consumerGroup);
        result.put("total", total);
        result.put("succeeded", succeeded);
        result.put("processing", processing);
        result.put("pending", pendingReady);
        result.put("retrying", retrying);
        result.put("dead", dead);
        result.put("conserved", accounted == total);
        result.put("accounted", accounted);
        return result;
    }
}
