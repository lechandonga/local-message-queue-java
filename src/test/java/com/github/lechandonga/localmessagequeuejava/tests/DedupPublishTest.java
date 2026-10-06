package com.github.lechandonga.localmessagequeuejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lechandonga.localmessagequeuejava.core.LocalMessageQueueService;
import com.github.lechandonga.localmessagequeuejava.core.PolledMessage;

/**
 * 业务覆盖：相同去重键重复投递必须幂等，调用方可区分新入队/被去重，
 * 且不会产生第二条待消费消息。
 */
class DedupPublishTest extends LocalMessageQueueTestBase {

    private static final Logger log = LoggerFactory.getLogger(DedupPublishTest.class);

    @Autowired
    private LocalMessageQueueService service;

    @Test
    @DisplayName("重复投递幂等：created=false 且只有一条待消费消息")
    void duplicatePublishIsIdempotent() {
        log.info("[用例开始] 重复投递幂等：同 topic + 同 dedupKey 投递两次");

        var first = service.publish("order-events", "order-1001", "创建订单");
        var second = service.publish("order-events", "order-1001", "创建订单(重复内容)");

        assertTrue(first.created(), "首次投递应返回 created=true（新入队）");
        assertFalse(second.created(), "重复投递应返回 created=false（被去重）");
        assertEquals(first.messageId(), second.messageId(), "去重时必须复用原消息 ID");

        Long messageCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM mq_message WHERE topic='order-events'", Long.class);
        assertEquals(1L, messageCount, "被去重后不得产生第二条消息");

        PolledMessage polled = service.poll("billing-group", "c1");
        assertEquals("创建订单", polled.payload(), "消费到的必须是首次投递的内容");
        assertTrue(service.ack(polled.groupStateId(), polled.leaseToken()));
        assertEquals(0L, service.poll("billing-group", "c1") == null ? 0L : 1L,
                "确认后队列应为空");

        // 不带去重键的消息不去重
        var a = service.publish("logs", null, "a");
        var b = service.publish("logs", null, "b");
        assertTrue(a.created() && b.created() && !a.messageId().equals(b.messageId()));

        // 相同 dedupKey 在不同 topic 下互不影响
        var otherTopic = service.publish("other-topic", "order-1001", "其他主题");
        assertTrue(otherTopic.created());
        assertNotEquals(first.messageId(), otherTopic.messageId());

        log.info("[用例通过] 首次 created=true，重复 created=false，消息表仅 1 条，消费内容为首次内容");
    }
}
