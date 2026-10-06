package com.github.lechandonga.localmessagequeuejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地消息队列可配置项，前缀 {@code lq}。
 */
@ConfigurationProperties(prefix = "lq")
public class LocalQueueProperties {

    /** H2 数据文件目录。 */
    private String dataDir = "./data";

    /** 消费租约时长（毫秒），超时未确认的消息会被回收重新投递。 */
    private long leaseMs = 30_000;

    /** 租约回收扫描间隔（毫秒）。 */
    private long reapIntervalMs = 1_000;

    /** 最大重试次数，超过后进入死信。 */
    private int maxRetries = 3;

    /** 重试初始退避（毫秒）。 */
    private long retryBackoffBaseMs = 1_000;

    /** 重试退避上限（毫秒）。 */
    private long retryBackoffMaxMs = 60_000;

    /** 首次投递可见延迟（毫秒）。 */
    private long defaultVisibleDelayMs = 0;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public long getLeaseMs() {
        return leaseMs;
    }

    public void setLeaseMs(long leaseMs) {
        this.leaseMs = leaseMs;
    }

    public long getReapIntervalMs() {
        return reapIntervalMs;
    }

    public void setReapIntervalMs(long reapIntervalMs) {
        this.reapIntervalMs = reapIntervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryBackoffBaseMs() {
        return retryBackoffBaseMs;
    }

    public void setRetryBackoffBaseMs(long retryBackoffBaseMs) {
        this.retryBackoffBaseMs = retryBackoffBaseMs;
    }

    public long getRetryBackoffMaxMs() {
        return retryBackoffMaxMs;
    }

    public void setRetryBackoffMaxMs(long retryBackoffMaxMs) {
        this.retryBackoffMaxMs = retryBackoffMaxMs;
    }

    public long getDefaultVisibleDelayMs() {
        return defaultVisibleDelayMs;
    }

    public void setDefaultVisibleDelayMs(long defaultVisibleDelayMs) {
        this.defaultVisibleDelayMs = defaultVisibleDelayMs;
    }
}
