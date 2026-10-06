package com.github.lechandonga.localmessagequeuejava.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 负责租约回收与停机释放。
 *
 * <p>正常停机：收到 Spring 关闭信号时把处理中消息全部释放回可投递。
 * 进程崩溃 / kill -9：依赖租约超时，后台任务或重启时回收。</p>
 */
@Component
public class QueueMaintenance {

    private static final Logger log = LoggerFactory.getLogger(QueueMaintenance.class);

    private final MessageQueueService queueService;

    public QueueMaintenance(MessageQueueService queueService) {
        this.queueService = queueService;
    }

    /** 启动时先回收一次上一进程遗留的处理中消息（重启恢复）。 */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverOnStartup() {
        int recovered = queueService.reclaimExpiredLocks();
        log.info("启动恢复检查完成，回收遗留处理中消息 {} 条（未确认消息不丢）", recovered);
    }

    /** 定时回收租约超时的处理中消息。 */
    @Scheduled(fixedDelayString = "${lmq.sweep-interval-ms:1000}")
    public void sweep() {
        queueService.reclaimExpiredLocks();
    }

    /**
     * 停机信号：把本进程所有处理中消息释放为可投递。
     * 监听 ContextClosedEvent 而非 @PreDestroy，确保在数据源销毁之前执行。
     */
    @EventListener(ContextClosedEvent.class)
    public void onShutdown() {
        log.info("收到停机信号，开始释放处理中消息...");
        queueService.releaseOnShutdown();
    }
}
