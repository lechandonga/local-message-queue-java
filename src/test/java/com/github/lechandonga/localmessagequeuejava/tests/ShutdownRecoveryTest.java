package com.github.lechandonga.localmessagequeuejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.github.lechandonga.localmessagequeuejava.Application;
import com.github.lechandonga.localmessagequeuejava.core.LocalMessageQueueService;
import com.github.lechandonga.localmessagequeuejava.core.PolledMessage;

/**
 * 业务覆盖：
 * 1. 处理中未确认（模拟进程崩溃），租约超时后消息自动回到可投递状态，旧租约的迟到确认无效；
 * 2. 优雅停机时处理中消息立即释放；进程重启后遗留的处理中消息恢复为可投递，消息不丢。
 */
class ShutdownRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(ShutdownRecoveryTest.class);

    @TempDir
    Path dataDir;

    private ConfigurableApplicationContext startContext(String port) {
        return new SpringApplicationBuilder(Application.class)
                .web(WebApplicationType.NONE)
                // 命令行参数优先级高于 test classpath 里的 application.properties，
                // 保证用例使用 @TempDir 下独立的数据目录
                .run(
                        "--lq.data-dir=" + dataDir,
                        "--lq.lease-ms=1000",
                        "--lq.reap-interval-ms=300",
                        "--lq.max-retries=3",
                        "--lq.retry-backoff-base-ms=10",
                        "--lq.retry-backoff-max-ms=1000",
                        "--server.port=" + port);
    }

    @Test
    @DisplayName("租约超时回收 + 停机释放 + 重启恢复，未确认消息一条不丢")
    void leaseReapAndRestartRecovery() throws Exception {
        log.info("[用例开始] 停机/重启恢复，数据目录={}", dataDir);
        ConfigurableApplicationContext ctx1 = startContext("0");
        LocalMessageQueueService service1 = ctx1.getBean(LocalMessageQueueService.class);

        service1.publish("recovery-topic", "r-1", "需要恢复的消息一");
        service1.publish("recovery-topic", "r-2", "需要恢复的消息二");

        // 消息一：被 poll 后消费者“异常退出”，既不 ack 也不 nack
        PolledMessage stolen = service1.poll("g1", "c1");
        assertNotNull(stolen);
        assertEquals("需要恢复的消息一", stolen.payload());

        // 同一时刻同组的其他消费者不能拿到同一条消息，只能拿到消息二
        PolledMessage other = service1.poll("g1", "c2");
        assertNotNull(other);
        assertEquals("需要恢复的消息二", other.payload());

        log.info("[崩溃模拟] 消息一处理中未确认，等待租约超时（1s）后被回收");
        Thread.sleep(2200);
        PolledMessage redelivered = service1.poll("g1", "c2");
        assertNotNull(redelivered, "租约超时后消息必须自动回到可投递状态");
        assertEquals(stolen.messageId(), redelivered.messageId(), "回到队列的是同一条消息");
        assertEquals(2, redelivered.attempts(), "租约回收不消耗重试次数，attempts 累加投递次数");
        // 旧租约 token 已失效，迟到的 ack 不能把消息算成功
        boolean lateAck = service1.ack(stolen.groupStateId(), stolen.leaseToken());
        assertFalse(lateAck, "旧租约的迟到确认必须被拒绝，不能误计成功");
        assertTrue(service1.ack(redelivered.groupStateId(), redelivered.leaseToken()));

        // 消息二保持处理中，随后优雅停机（模拟 SIGTERM）
        log.info("[停机模拟] 消息二正在处理中，关闭上下文触发优雅释放");
        ctx1.close();

        ConfigurableApplicationContext ctx2 = startContext("0");
        LocalMessageQueueService service2 = ctx2.getBean(LocalMessageQueueService.class);
        PolledMessage afterRestart = service2.poll("g1", "c-after-restart");
        assertNotNull(afterRestart, "重启后未确认消息必须恢复，不能丢失");
        assertEquals("需要恢复的消息二", afterRestart.payload());
        assertTrue(service2.ack(afterRestart.groupStateId(), afterRestart.leaseToken()));
        assertNull(service2.poll("g1", "c"), "全部确认后队列应为空");

        var stats = service2.stats("recovery-topic", "g1");
        assertEquals(2L, ((Number) stats.get("total")).longValue());
        assertEquals(2L, ((Number) stats.get("succeeded")).longValue());
        assertEquals(Boolean.TRUE, stats.get("conserved"));
        ctx2.close();
        log.info("[用例通过] 租约回收、停机释放、重启恢复均不丢消息，迟到 ack 被拒绝");
    }
}
