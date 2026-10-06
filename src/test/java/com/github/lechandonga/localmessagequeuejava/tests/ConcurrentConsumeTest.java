package com.github.lechandonga.localmessagequeuejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lechandonga.localmessagequeuejava.core.LocalMessageQueueService;
import com.github.lechandonga.localmessagequeuejava.core.PolledMessage;

/**
 * 业务覆盖：多生产者、同组多消费者并发运行时，
 * 一条消息同一时刻只能被一个消费者处理，最终数量守恒；
 * 两个消费者组各自独立消费同一份消息。
 */
class ConcurrentConsumeTest extends LocalMessageQueueTestBase {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentConsumeTest.class);

    @Autowired
    private LocalMessageQueueService service;

    @Test
    @DisplayName("多生产者+多消费者并发：无重复处理，两个组各自全部消费成功，数量守恒")
    void concurrentProducersAndConsumers() throws Exception {
        int producers = 4;
        int consumersPerGroup = 6;
        int perProducer = 125;
        int total = producers * perProducer;
        String runId = java.util.UUID.randomUUID().toString().substring(0, 8);
        log.info("[用例开始] {} 生产者 x {} 条，{} 消费者/组，两个消费者组，共投递 {} 条",
                producers, perProducer, consumersPerGroup, total);

        ExecutorService pool = Executors.newFixedThreadPool(producers + consumersPerGroup * 2);
        CountDownLatch producersDone = new CountDownLatch(producers);
        for (int p = 0; p < producers; p++) {
            int producerId = p;
            pool.submit(() -> {
                    try {
                        for (int i = 0; i < perProducer; i++) {
                            service.publish("concurrency-topic-" + runId,
                                    runId + "-p" + producerId + "-m" + i,
                                    "payload-" + producerId + "-" + i);
                    }
                } finally {
                    producersDone.countDown();
                }
            });
        }

        Map<String, Set<Long>> handledStateIds = new ConcurrentHashMap<>();
        Map<String, Set<Long>> handledMessageIds = new ConcurrentHashMap<>();
        // messageId -> 当前持有者租约 token；poll 时放入，ack/nack 后清除。
        // 若 poll 到一条仍有持有者的消息，说明同一时刻被两个消费者处理。
        Map<String, Map<Long, String>> currentHolder = new ConcurrentHashMap<>();
        AtomicInteger simultaneousHandling = new AtomicInteger();
        CountDownLatch consumersDone = new CountDownLatch(consumersPerGroup * 2);

        for (String group : List.of("group-A", "group-B")) {
            handledStateIds.put(group, Collections.newSetFromMap(new ConcurrentHashMap<>()));
            handledMessageIds.put(group, Collections.newSetFromMap(new ConcurrentHashMap<>()));
            currentHolder.put(group, new ConcurrentHashMap<>());
            for (int c = 0; c < consumersPerGroup; c++) {
            String consumerId = group + "-consumer-" + c;
                pool.submit(() -> {
                    try {
                        while (true) {
                            PolledMessage msg = service.poll(group, consumerId);
                            String topic = "concurrency-topic-" + runId;
                            if (msg == null) {
                                if (producersDone.getCount() == 0
                                        && groupSucceeded(group, topic) >= total) {
                                    break;
                                }
                                Thread.sleep(5);
                                continue;
                            }
                            // groupStateId 每次投递唯一，绝不可能重复
                            if (!handledStateIds.get(group).add(msg.groupStateId())) {
                                simultaneousHandling.incrementAndGet();
                            }
                            if (!handledMessageIds.get(group).add(msg.messageId())) {
                                simultaneousHandling.incrementAndGet();
                            }
                            String previousHolder = currentHolder.get(group)
                                    .put(msg.messageId(), msg.leaseToken());
                            if (previousHolder != null) {
                                simultaneousHandling.incrementAndGet();
                                log.warn("[冲突] group={} messageId={} 新租约={} 未释放的旧租约={} stateId={}",
                                        group, msg.messageId(), msg.leaseToken(),
                                        previousHolder, msg.groupStateId());
                            }
                            // 本用例专注验证并发互斥与数量守恒，全部确认；
                            // 重试/死信语义在 RetryDeadLetterTest 中单独覆盖。
                            service.ack(msg.groupStateId(), msg.leaseToken());
                            currentHolder.get(group).remove(msg.messageId(), msg.leaseToken());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        consumersDone.countDown();
                    }
                });
            }
        }

        assertTrue(producersDone.await(30, TimeUnit.SECONDS), "生产者应在超时前完成");
        assertTrue(consumersDone.await(60, TimeUnit.SECONDS), "消费者应在超时前全部消费完");
        pool.shutdownNow();

        assertEquals(0, simultaneousHandling.get(), "同组内同一条消息不得被两个消费者同时处理");
        assertEquals(total, handledMessageIds.get("group-A").size(), "组A应处理完全部消息且无重复");
        assertEquals(total, handledMessageIds.get("group-B").size(), "组B应独立处理完全部消息");

        String topic = "concurrency-topic-" + runId;
        Map<String, Object> stats = service.stats(topic, null);
        log.info("[统计] {}", stats);
        assertEquals(total, ((Number) stats.get("total")).longValue());
        // 不指定组时按消息维度聚合：500 条消息在两个组都成功 => succeeded=500
        assertEquals(total, ((Number) stats.get("succeeded")).longValue());
        assertEquals(0L, ((Number) stats.get("processing")).longValue());
        assertEquals(0L, ((Number) stats.get("pending")).longValue());
        assertEquals(0L, ((Number) stats.get("retrying")).longValue());
        assertEquals(0L, ((Number) stats.get("dead")).longValue());
        assertEquals(Boolean.TRUE, stats.get("conserved"),
                "成功+处理中+待投递+重试中+死信必须守恒");
        // 指定具体组时按组进度统计：每个组独立成功 500
        assertEquals(total,
                ((Number) service.stats(topic, "group-A").get("succeeded")).longValue());
        assertEquals(total,
                ((Number) service.stats(topic, "group-B").get("succeeded")).longValue());
        log.info("[用例通过] {} 条消息 x 2 组全部成功，无重复处理，数量守恒", total);
    }

    private long groupSucceeded(String group, String topic) {
        Map<String, Object> stats = service.stats(topic, group);
        return ((Number) stats.get("succeeded")).longValue();
    }
}
