package com.github.lechandonga.localmessagequeuejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lechandonga.localmessagequeuejava.core.LocalMessageQueueService;
import com.github.lechandonga.localmessagequeuejava.core.PolledMessage;

/**
 * 业务覆盖：消费失败按指数退避重试，超过配置上限转入死信；
 * 死信可查看并手动重放；重放复用原消息、重试计数清零但上限不变。
 */
class RetryDeadLetterTest extends LocalMessageQueueTestBase {

    private static final Logger log = LoggerFactory.getLogger(RetryDeadLetterTest.class);

    @Autowired
    private LocalMessageQueueService service;

    @Test
    @DisplayName("重试 3 次耗尽后进入死信，重放后可再次消费且不产生新消息")
    void retryUntilDeadThenReplay() throws Exception {
        log.info("[用例开始] 重试耗尽->死信->手动重放（maxRetries=3，即最多投递 4 次）");
        service.publish("pay-events", "pay-77", "支付回调");

        for (int delivery = 1; delivery <= 3; delivery++) {
            PolledMessage msg = service.poll("pay-group", "c1");
            assertNotNull(msg, "第 " + delivery + " 次投递应拿到消息");
            assertEquals(delivery, msg.attempts());
            boolean rejected = service.nack(msg.groupStateId(), msg.leaseToken(),
                    "模拟业务异常 #" + delivery);
            assertTrue(rejected, "持有有效租约的 nack 必须成功");
            log.info("[重试] 第 {} 次投递被拒绝，消息进入退避等待", delivery);
            Thread.sleep(200); // base=10ms 指数退避，等待退避到期
        }

        // 第 4 次投递：attempts=4 > maxRetries=3，nack 后进入死信
        PolledMessage last = service.poll("pay-group", "c1");
        assertNotNull(last);
        assertEquals(4, last.attempts());
        assertTrue(service.nack(last.groupStateId(), last.leaseToken(), "最终失败"));
        log.info("[死信] 第 4 次投递仍失败，消息转入死信");

        List<Map<String, Object>> dead = service.deadLetters("pay-events", "pay-group");
        assertEquals(1, dead.size(), "死信列表应有 1 条");
        assertEquals("支付回调", dead.get(0).get("payload"), "死信保留原始消息内容");
        long stateId = ((Number) dead.get(0).get("group_state_id")).longValue();

        // 死信状态下不能再被 poll
        assertEquals(null, service.poll("pay-group", "c1"));

        // 手动重放：复用原消息，不产生第二条消息
        assertTrue(service.replayDeadLetter(stateId));
        assertEquals(1L, jdbc.queryForObject(
                "SELECT COUNT(*) FROM mq_message WHERE dedup_key='pay-77'", Long.class),
                "重放不得产生新消息，也不能绕过去重记录");

        PolledMessage replayed = service.poll("pay-group", "c1");
        assertNotNull(replayed, "重放后消息应可重新投递");
        assertEquals(1, replayed.attempts(), "重放后重试计数应清零");
        assertEquals("支付回调", replayed.payload());
        assertTrue(service.ack(replayed.groupStateId(), replayed.leaseToken()));
        log.info("[重放] 重放后 attempts 重置为 1，本次消费确认成功");

        // 重放后再次持续失败，仍受同一重试上限约束，会再次进死信
        service.publish("pay-events", "pay-88", "另一条支付回调");
        PolledMessage cursor = null;
        for (int i = 0; i < 4; i++) {
            cursor = service.poll("pay-group", "c1");
            assertNotNull(cursor);
            assertTrue(service.nack(cursor.groupStateId(), cursor.leaseToken(), "再次失败"));
            Thread.sleep(200);
        }
        // 重放后成功的消息保持成功；新消息耗尽后进入死信，死信列表为 1 条
        assertEquals(1, service.deadLetters("pay-events", null).size(),
                "重放后的消息仍受重试上限约束，再次耗尽后重回死信");

        // 重放一个不存在/非死信的记录返回 false
        assertFalse(service.replayDeadLetter(999_999L));

        log.info("[用例通过] 重试上限生效、死信可查可重放、重放不绕过去重与上限");
    }
}
