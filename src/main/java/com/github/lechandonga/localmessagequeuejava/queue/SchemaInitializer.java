package com.github.lechandonga.localmessagequeuejava.queue;

import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 本地存储结构初始化。所有表都建在嵌入式 H2 文件库中，随进程启停，无需外部中间件。
 */
@Component
public class SchemaInitializer {

    private final JdbcTemplate jdbcTemplate;

    public SchemaInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void init() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS lmq_message (
                    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
                    topic        VARCHAR(255) NOT NULL,
                    body         CLOB NOT NULL,
                    dedup_key    VARCHAR(255),
                    created_at   BIGINT NOT NULL,
                    CONSTRAINT lmq_message_dedup_uk UNIQUE (topic, dedup_key)
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS lmq_group (
                    name        VARCHAR(255) PRIMARY KEY,
                    created_at  BIGINT NOT NULL
                )
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS lmq_delivery (
                    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
                    message_id      BIGINT NOT NULL,
                    topic           VARCHAR(255) NOT NULL,
                    grp             VARCHAR(255) NOT NULL,
                    status          VARCHAR(16) NOT NULL,
                    attempts        INT NOT NULL DEFAULT 0,
                    available_at    BIGINT NOT NULL,
                    locked_by       VARCHAR(128),
                    locked_at       BIGINT,
                    updated_at      BIGINT NOT NULL,
                    last_error      VARCHAR(2000),
                    CONSTRAINT lmq_delivery_uk UNIQUE (message_id, grp)
                )
                """);
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS lmx_delivery_poll "
                + "ON lmq_delivery (grp, status, available_at)");
    }
}
