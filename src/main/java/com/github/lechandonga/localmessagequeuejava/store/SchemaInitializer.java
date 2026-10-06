package com.github.lechandonga.localmessagequeuejava.store;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 本地表结构初始化。
 * <ul>
 *   <li>mq_message：消息内容与去重记录（topic + dedupKey 唯一）。</li>
 *   <li>mq_group_state：每个消费者组对每条消息的消费进度。</li>
 * </ul>
 */
@Component
public class SchemaInitializer {

    public SchemaInitializer(JdbcTemplate jdbc) {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS mq_message (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    topic VARCHAR(255) NOT NULL,
                    -- 不去重的消息在入库时写入 UUID，保证唯一约束不误伤无 dedupKey 的消息
                    dedup_key VARCHAR(255) NOT NULL,
                    dedup_key_provided BOOLEAN NOT NULL,
                    payload CLOB NOT NULL,
                    created_at BIGINT NOT NULL,
                    CONSTRAINT uk_topic_dedup UNIQUE (topic, dedup_key)
                )
                """);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS mq_group_state (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    topic VARCHAR(255) NOT NULL,
                    consumer_group VARCHAR(255) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    attempts INT NOT NULL DEFAULT 0,
                    visible_at BIGINT NOT NULL,
                    lease_expires_at BIGINT NOT NULL DEFAULT 0,
                    lease_token VARCHAR(64),
                    consumer_id VARCHAR(255),
                    last_error CLOB,
                    updated_at BIGINT NOT NULL,
                    CONSTRAINT uk_msg_group UNIQUE (message_id, consumer_group)
                )
                """);
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_group_dispatch ON mq_group_state "
                + "(consumer_group, status, visible_at)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_group_lease ON mq_group_state "
                + "(consumer_group, status, lease_expires_at)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_message_topic ON mq_message (topic)");
    }
}
