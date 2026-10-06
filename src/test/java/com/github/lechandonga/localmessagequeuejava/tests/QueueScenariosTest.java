package com.github.lechandonga.localmessagequeuejava.tests;

import com.github.lechandonga.localmessagequeuejava.queue.DeliveryView;
import com.github.lechandonga.localmessagequeuejava.queue.MessageQueueService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地消息队列五大业务场景：
 * 1) 重复投递幂等去重 2) 重试耗尽进死信 3) 死信重放不绕过上限
 * 4) 多生产者多消费者并发，数量守恒且不会重复处理 5) 停机/重启后未确认消息恢复。
 */
@SpringBootTest
class QueueScenariosTest {

    private static final Logger log = LoggerFactory.getLogger("SCENARIO-TEST");

    @Autowired
    private MessageQueueService queue;

    @Autowired
    private JdbcTemplate jdbc;

    private String uniqueTopic() {
        return "topic-" + UUID.randomUUID();
    }

    /** 文件库会持久化，每个场景开始前清空，保证用例独立可重复运行。 */
    @BeforeEach
    void cleanTables() {
        jdbc.update("DELETE FROM lmq_delivery");
        jdbc.update("DELETE FROM lmq_message");
        jdbc.update("DELETE FROM lmq_group");
    }

    private long number(Object value) {
        return ((Number) value).longValue();
    }

    @Test
    @DisplayName("场景1-重复投递幂等去重")
    void duplicatePublishIsIdempotent() {
        log.info(">>> [场景1] 开始：相同去重键重复投递必须幂等，不得产生第二条待消费消息");
        String topic = uniqueTopic();
        String group = "g1";
        String dedupKey = "order-1001";
        queue.registerGroup(group);

        Object[] first = queue.publish(topic, "pay 100", dedupKey);
        Object[] second = queue.publish(topic, "pay 100 changed", dedupKey);
        Object[] thirdNoKey = queue.publish(topic, "no dedup", null);

        assertEquals(true, first[1], "首次投递应为新入队 created=true");
        assertEquals(false, second[1], "重复去重键应返回 created=false（deduplicated=true）");
        assertEquals(first[0], second[0], "重复投递必须复用同一条消息ID");
        assertEquals(true, thirdNoKey[1], "无去重键的消息正常入队");

        Map<String, Object> stats = queue.stats(group);
        assertEquals(2L, stats.get("total"),
                "该组只应有 2 条待消费（去重消息 + 无键消息），重复投递不得产生第二条");
        log.info("<<< [场景1] 结果=通过：两次相同去重键复用消息#{}，组内 total={} 无重复",
                first[0], stats.get("total"));
    }

    @Test
    @DisplayName("场景2-重试耗尽进入死信")
    void retriesExhaustedGoesDead() {
        log.info(">>> [场景2] 开始：消费失败按退避重试，超过 maxRetries 后进入死信");
        String topic = uniqueTopic();
        String group = "g2";
        queue.registerGroup(group);
        Object[] published = queue.publish(topic, "flaky task", null);

        DeliveryView d = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            d = pollAvailable(group, "fail-consumer");
            assertNotNull(d, "退避到期后应能再次领取到消息，第 " + attempt + " 次");
            assertTrue(queue.reject(d.getDeliveryId(), "fail-consumer", "boom " + attempt));
            DeliveryView after = queue.getDelivery(d.getDeliveryId());
            assertEquals("RETRYING", after.getStatus(), "第 " + attempt + " 次失败应进入重试");
            log.info("[场景2] 第 {} 次业务失败 -> RETRYING，attempts={}", attempt, after.getAttempts());
        }
        DeliveryView fourth = pollAvailable(group, "fail-consumer");
        assertNotNull(fourth);
        queue.reject(fourth.getDeliveryId(), "fail-consumer", "boom final");
        DeliveryView dead = queue.getDelivery(fourth.getDeliveryId());
        assertEquals("DEAD", dead.getStatus(), "超过重试上限必须进入死信");

        List<DeliveryView> deadList = queue.listDead(group);
        assertTrue(deadList.stream().anyMatch(x -> x.getDeliveryId() == dead.getDeliveryId()),
                "死信必须能被单独查询到");
        Map<String, Object> stats = queue.stats(group);
        assertEquals(1L, stats.get("dead"));
        assertEquals(1L, stats.get("total"), "成功+重试+待投递+死信之和守恒");
        log.info("<<< [场景2] 结果=通过：消息#{} 第4次失败后进入死信，死信列表可查，total=1",
                published[0]);
    }

    @Test
    @DisplayName("场景3-死信重放不绕过重试上限")
    void deadReplayKeepsAttemptsAndFailsAgain() {
        log.info(">>> [场景3] 开始：死信手动重放保留 attempts，再失败立即回死信，且不产生新消息");
        String topic = uniqueTopic();
        String group = "g3";
        queue.registerGroup(group);
        queue.publish(topic, "replay me", null);

        DeliveryView d;
        for (int i = 0; i < 4; i++) {
            d = pollAvailable(group, "c");
            queue.reject(d.getDeliveryId(), "c", "fail");
        }
        d = queue.listDead(group).stream()
                .filter(x -> topic.equals(x.getTopic())).findFirst().orElseThrow();
        int attemptsBefore = d.getAttempts();

        assertTrue(queue.replayDead(d.getDeliveryId()));
        DeliveryView replayed = queue.getDelivery(d.getDeliveryId());
        assertEquals("RETRYING", replayed.getStatus());
        assertEquals(attemptsBefore, replayed.getAttempts(),
                "重放不得重置重试次数（不能绕过重试上限）");

        DeliveryView again = pollAvailable(group, "c");
        assertNotNull(again, "退避到期后重放消息必须可再次被领取");
        assertTrue(queue.reject(again.getDeliveryId(), "c", "still failing"));
        assertEquals("DEAD", queue.getDelivery(d.getDeliveryId()).getStatus(),
                "attempts 已达上限，再失败一次立即回到死信");

        Map<String, Object> stats = queue.stats(group);
        assertEquals(1L, stats.get("total"), "重放不得产生新消息，总量不变");
        assertEquals(1L, stats.get("dead"));
        log.info("<<< [场景3] 结果=通过：死信重放保留 attempts={}，再次失败立即回死信，total 仍为 1",
                attemptsBefore);
    }

    @Test
    @DisplayName("场景4-多生产者多消费者并发数量守恒不重复")
    void concurrentProducersAndConsumers() throws Exception {
        log.info(">>> [场景4] 开始：多生产者并发投递 + 多消费者并发领取/确认/失败重试");
        String topic = uniqueTopic();
        String group = "g4";
        queue.registerGroup(group);

        int producers = 4;
        int perProducer = 100;
        int totalMessages = producers * perProducer;
        int consumers = 4;

        ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);
        CountDownLatch producersDone = new CountDownLatch(producers);
        AtomicInteger duplicated = new AtomicInteger();
        for (int p = 0; p < producers; p++) {
            int producerId = p;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < perProducer; i++) {
                        Object[] r = queue.publish(topic, "msg-" + producerId + "-" + i, null);
                        if (!Boolean.TRUE.equals(r[1])) {
                            duplicated.incrementAndGet();
                        }
                    }
                } finally {
                    producersDone.countDown();
                }
            });
        }

        // 记录每条投递当前的持锁消费者：验证同一时刻不会被两个消费者同时处理。
        // 失败后的顺序重投是合法行为（旧锁已在 reject 时释放），不在此列。
        ConcurrentHashMap<Long, String> activeOwners = new ConcurrentHashMap<>();
        java.util.Set<Long> concurrentViolations = ConcurrentHashMap.newKeySet();
        AtomicLong successCount = new AtomicLong();
        CountDownLatch consumersStop = new CountDownLatch(consumers);
        for (int c = 0; c < consumers; c++) {
            String consumer = "consumer-" + c;
            pool.submit(() -> {
                try {
                    long idleSince = -1;
                    while (true) {
                        DeliveryView d = queue.poll(group, consumer);
                        if (d == null) {
                            // 生产者已结束且没有任何待处理/处理中消息，只剩成功/重试/死信则退出
                            if (producersDone.getCount() == 0) {
                                Map<String, Object> s = queue.stats(group);
                                long active = number(s.get("pending")) + number(s.get("inFlight"));
                                if (active == 0) {
                                    if (idleSince < 0) {
                                        idleSince = System.currentTimeMillis();
                                    } else if (System.currentTimeMillis() - idleSince > 2000) {
                                        break;
                                    }
                                } else {
                                    idleSince = -1;
                                }
                            }
                            sleep(10);
                            continue;
                        }
                        String previousOwner = activeOwners.putIfAbsent(d.getDeliveryId(), consumer);
                        if (previousOwner != null && !previousOwner.equals(consumer)) {
                            concurrentViolations.add(d.getDeliveryId());
                            log.error("并发冲突！投递#{} 同时被 [{}] 和 [{}] 持有",
                                    d.getDeliveryId(), previousOwner, consumer);
                        }
                        if (d.getAttempts() == 0 && d.getDeliveryId() % 10 == 0) {
                            if (queue.reject(d.getDeliveryId(), consumer, "transient failure")) {
                                activeOwners.remove(d.getDeliveryId());
                            }
                        } else if (queue.ack(d.getDeliveryId(), consumer)) {
                            activeOwners.remove(d.getDeliveryId());
                            successCount.incrementAndGet();
                        }
                    }
                } finally {
                    consumersStop.countDown();
                }
            });
        }
        assertTrue(producersDone.await(30, TimeUnit.SECONDS), "生产者应在限定时间完成");
        assertTrue(consumersStop.await(60, TimeUnit.SECONDS), "消费者应收敛退出");
        pool.shutdown();

        // 消费者退出后，失败过一次的消息处于退避重试中，等待后台到期并由“补漏”循环全部确认
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50)).until(() -> {
            drainRetrying(group, "cleanup");
            return queue.stats(group).get("succeeded").equals((long) totalMessages);
        });

        Map<String, Object> stats = queue.stats(group);
        assertEquals(0, duplicated.get(), "无去重键并发投递不应触发去重");
        assertEquals(0, concurrentViolations.size(),
                "同一条消息在同一时刻不得被两个消费者同时处理: " + concurrentViolations);
        assertEquals(0, activeOwners.size(), "收敛后不应有残留持锁");
        assertEquals(totalMessages, successCount.get(), "每条消息恰好成功一次");
        assertEquals((long) totalMessages, number(stats.get("succeeded")));
        assertEquals((long) totalMessages, stats.get("total"), "数量守恒：五类状态之和=投递总量");
        assertEquals(0L, stats.get("inFlight"));
        assertEquals(0L, stats.get("pending"));
        assertEquals(0L, stats.get("retrying"));
        assertEquals(0L, stats.get("dead"));
        log.info("<<< [场景4] 结果=通过：{}生产者/{}消费者并发 {} 条消息，全部成功、无重复处理、五类状态守恒",
                producers, consumers, totalMessages);
    }

    @Test
    @DisplayName("场景5-停机释放与重启恢复未确认消息")
    void shutdownAndRestartRecovery() {
        log.info(">>> [场景5] 开始：停机时处理中消息回到可投递；崩溃后重启未确认消息不丢");
        String topic = uniqueTopic();
        String group = "g5";
        queue.registerGroup(group);
        for (int i = 0; i < 10; i++) {
            queue.publish(topic, "shutdown-" + i, null);
        }

        DeliveryView first = queue.poll(group, "c1");
        DeliveryView second = queue.poll(group, "c1");
        DeliveryView third = queue.poll(group, "c1");
        assertNotNull(first);
        assertNotNull(second);
        assertNotNull(third);
        queue.ack(first.getDeliveryId(), "c1");

        // 正常停机：未确认的处理中消息必须释放回可投递
        int released = queue.releaseOnShutdown();
        assertTrue(released >= 2, "停机应释放未确认的处理中消息");
        assertEquals("PENDING", queue.getDelivery(second.getDeliveryId()).getStatus());
        assertEquals("PENDING", queue.getDelivery(third.getDeliveryId()).getStatus());

        // 模拟进程崩溃：领取后不确认、不走停机钩子；重启后靠租约超时回收
        DeliveryView locked = queue.poll(group, "c2");
        assertNotNull(locked);
        long lockedId = locked.getDeliveryId();
        jdbc.update("UPDATE lmq_delivery SET locked_at = ? WHERE id = ?",
                System.currentTimeMillis() - 3_600_000L, lockedId);
        int reclaimed = queue.reclaimExpiredLocks();
        assertTrue(reclaimed >= 1, "重启时应回收上一进程遗留的租约超时消息");
        assertEquals("PENDING", queue.getDelivery(lockedId).getStatus(),
                "崩溃前未确认的消息重启后必须可重新投递，不能静默丢失");

        Map<String, Object> stats = queue.stats(group);
        assertEquals(10L, stats.get("total"), "恢复过程中数量依旧守恒");
        assertEquals(1L, stats.get("succeeded"));
        assertEquals(0L, stats.get("inFlight"), "不得有处理中消息残留");
        assertEquals(9L, number(stats.get("pending")) + number(stats.get("retrying")),
                "其余 9 条全部处于可投递/重试中，没有一条被计入成功");
        log.info("<<< [场景5] 结果=通过：停机释放 {} 条、重启租约回收 {} 条，未确认消息全部恢复，total=10 守恒",
                released, reclaimed);
    }

    /** 场景4收尾用：把退避重试中的消息全部领取并确认。 */
    private void drainRetrying(String group, String consumer) {
        DeliveryView d;
        while ((d = queue.poll(group, consumer)) != null) {
            queue.ack(d.getDeliveryId(), consumer);
        }
    }

    private DeliveryView pollAvailable(String group, String consumer) {
        for (int i = 0; i < 400; i++) {
            DeliveryView d = queue.poll(group, consumer);
            if (d != null) {
                return d;
            }
            sleep(25);
        }
        return null;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
