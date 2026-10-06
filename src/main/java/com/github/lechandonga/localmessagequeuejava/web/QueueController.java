package com.github.lechandonga.localmessagequeuejava.web;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.lechandonga.localmessagequeuejava.core.LocalMessageQueueService;
import com.github.lechandonga.localmessagequeuejava.core.PolledMessage;

/**
 * 本地消息队列 HTTP 入口，方便脚本/curl 复现投递、消费、确认与状态查询。
 */
@RestController
public class QueueController {

    private final LocalMessageQueueService service;

    public QueueController(LocalMessageQueueService service) {
        this.service = service;
    }

    @PostMapping("/api/queues/messages")
    public Map<String, Object> publish(@RequestBody Map<String, Object> body) {
        String topic = str(body.get("topic"));
        String dedupKey = str(body.get("dedupKey"));
        String payload = str(body.get("payload"));
        LocalMessageQueueService.PublishResult result = service.publish(topic, dedupKey, payload);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("messageId", result.messageId());
        // created=true 表示新入队，false 表示命中去重（复用旧消息，不产生第二条待消费消息）
        resp.put("created", result.created());
        resp.put("deduped", !result.created());
        return resp;
    }

    @PostMapping("/api/queues/poll")
    public ResponseEntity<Map<String, Object>> poll(@RequestBody(required = false) Map<String, Object> body) {
        Map<String, Object> req = body == null ? Map.of() : body;
        String group = str(req.get("consumerGroup"));
        String consumerId = str(req.get("consumerId"));
        PolledMessage msg = service.poll(group, consumerId);
        if (msg == null) {
            return ResponseEntity.noContent().build();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("groupStateId", msg.groupStateId());
        resp.put("messageId", msg.messageId());
        resp.put("topic", msg.topic());
        resp.put("consumerGroup", msg.consumerGroup());
        resp.put("consumerId", msg.consumerId());
        resp.put("payload", msg.payload());
        resp.put("attempts", msg.attempts());
        resp.put("leaseToken", msg.leaseToken());
        resp.put("leaseExpiresAt", msg.leaseExpiresAt());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/api/queues/{groupStateId}/ack")
    public Map<String, Object> ack(@PathVariable long groupStateId,
                                   @RequestBody Map<String, Object> body) {
        boolean ok = service.ack(groupStateId, str(body.get("leaseToken")));
        if (!ok) {
            throw new LeaseMismatchException("ack 失败：租约不存在或已过期，消息已可被重新投递");
        }
        return Map.of("acknowledged", true);
    }

    @PostMapping("/api/queues/{groupStateId}/nack")
    public Map<String, Object> nack(@PathVariable long groupStateId,
                                    @RequestBody Map<String, Object> body) {
        boolean ok = service.nack(groupStateId, str(body.get("leaseToken")), str(body.get("error")));
        if (!ok) {
            throw new LeaseMismatchException("nack 失败：租约不存在或已过期，消息已可被重新投递");
        }
        return Map.of("rejected", true);
    }

    @GetMapping("/api/queues/messages/{messageId}")
    public ResponseEntity<Map<String, Object>> status(@PathVariable long messageId) {
        Map<String, Object> status = service.messageStatus(messageId);
        return status == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(status);
    }

    @GetMapping("/api/queues/dead-letters")
    public List<Map<String, Object>> deadLetters(@RequestParam(required = false) String topic,
                                                 @RequestParam(required = false) String consumerGroup) {
        return service.deadLetters(topic, consumerGroup);
    }

    @PostMapping("/api/queues/dead-letters/{groupStateId}/replay")
    public Map<String, Object> replay(@PathVariable long groupStateId) {
        boolean replayed = service.replayDeadLetter(groupStateId);
        if (!replayed) {
            throw new IllegalArgumentException("死信记录不存在或已不在死信状态: " + groupStateId);
        }
        return Map.of("replayed", true, "groupStateId", groupStateId);
    }

    @GetMapping("/api/queues/stats")
    public Map<String, Object> stats(@RequestParam(required = false) String topic,
                                     @RequestParam(required = false) String consumerGroup) {
        return service.stats(topic, consumerGroup);
    }

    private static String str(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    @ExceptionHandler(LeaseMismatchException.class)
    public ResponseEntity<Map<String, Object>> handleLease(LeaseMismatchException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "LEASE_MISMATCH", "message", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "BAD_REQUEST", "message", e.getMessage()));
    }

    static class LeaseMismatchException extends RuntimeException {
        LeaseMismatchException(String message) {
            super(message);
        }
    }
}
