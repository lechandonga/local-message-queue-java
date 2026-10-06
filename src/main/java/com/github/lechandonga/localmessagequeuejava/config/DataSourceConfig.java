package com.github.lechandonga.localmessagequeuejava.config;

import java.nio.file.Files;
import java.nio.file.Path;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * 嵌入式 H2（文件模式）数据源与事务管理，所有状态都保存在本地磁盘，不依赖外部中间件。
 */
@Configuration
@EnableTransactionManagement
@EnableConfigurationProperties(LocalQueueProperties.class)
public class DataSourceConfig {

    @Bean
    public DataSource dataSource(LocalQueueProperties properties) {
        Path dir = Path.of(properties.getDataDir()).toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (Exception e) {
            throw new IllegalStateException("无法创建数据目录: " + dir, e);
        }
        JdbcDataSource ds = new JdbcDataSource();
        // 纯嵌入式文件模式：锁文件与数据文件同目录，不同数据目录天然隔离。
        ds.setURL("jdbc:h2:file:" + dir.resolve("local-queue")
                + ";DB_CLOSE_DELAY=-1;MODE=LEGACY");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
