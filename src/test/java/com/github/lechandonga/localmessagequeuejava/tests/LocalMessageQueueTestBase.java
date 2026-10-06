package com.github.lechandonga.localmessagequeuejava.tests;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest
public abstract class LocalMessageQueueTestBase {

    // 整个测试 JVM 内唯一的数据目录，避免与默认/其他工程的库文件互相污染
    private static final String TEST_DATA_DIR =
            System.getProperty("java.io.tmpdir") + "/lq-tests-" + UUID.randomUUID();

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("lq.data-dir", () -> TEST_DATA_DIR);
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @BeforeEach
    void cleanTables() {
        jdbc.execute("DELETE FROM mq_group_state");
        jdbc.execute("DELETE FROM mq_message");
    }
}
