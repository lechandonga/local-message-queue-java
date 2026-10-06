package com.github.lechandonga.localmessagequeuejava.lifecycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.lechandonga.localmessagequeuejava.config.LocalQueueProperties;
import com.github.lechandonga.localmessagequeuejava.core.LocalMessageQueueService;
import com.github.lechandonga.localmessagequeuejava.store.SchemaInitializer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * 周期性回收租约超时的消息：消费者处理中异常、崩溃或进程被 kill -9 后，
 * 未确认消息会在 {@code lq.lease-ms} 后自动回到可投递状态，不会被静默丢弃。
 */
@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final LocalMessageQueueService service;
    private final LocalQueueProperties props;
    private ScheduledExecutorService executor;

    public LeaseReaper(LocalMessageQueueService service, LocalQueueProperties props,
                       SchemaInitializer schemaInitializer) {
        this.service = service;
        this.props = props;
    }

    @PostConstruct
    public void start() {
        int recovered = service.recoverOrphanedProcessing();
        if (recovered > 0) {
            log.info("[恢复] 启动时发现 {} 条上次进程遗留的处理中消息，已恢复为可投递", recovered);
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "local-queue-lease-reaper");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::reapSafely,
                props.getReapIntervalMs(), props.getReapIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void reapSafely() {
        try {
            int n = service.reapExpiredLeases();
            if (n > 0) {
                log.info("[回收] {} 条租约超时消息已回到可投递状态", n);
            }
        } catch (Exception e) {
            log.warn("[回收] 租约回收失败，将在下个周期重试: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
        try {
            int n = service.releaseProcessingOnShutdown();
            if (n > 0) {
                log.info("[停机] {} 条处理中消息已释放回可投递状态", n);
            }
        } catch (Exception e) {
            log.warn("[停机] 释放处理中消息失败，租约回收器会在重启后兜底: {}", e.getMessage());
        }
    }
}
