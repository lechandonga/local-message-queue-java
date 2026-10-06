package com.github.lechandonga.localmessagequeuejava.web;

import com.github.lechandonga.localmessagequeuejava.queue.DeliveryView;
import com.github.lechandonga.localmessagequeuejava.queue.LmqProperties;
import com.github.lechandonga.localmessagequeuejava.queue.MessageQueueService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地可调用入口（HTTP/JSON），方便脚本与命令行复现。
 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final MessageQueueService queueService;
    private final LmqProperties properties;

    public QueueController(MessageQueueService queueService, LmqProperties properties) {
        this.queueService = queueService;
        this.properties = properties;
    }

    /** 注册消费组（投递前注册可让该组收到全部历史消息；不注册也能直接消费）。 */
    @PostMapping("/groups/{group}")
    public Map<String, Object> registerGroup(@PathVariable String group) {
        queueService.registerGroup(group);
        return Map.of("group", group, "registered", true);
    }

    /**
     * 投递消息。
     * body: {"body": "内容", "dedupKey": "可选去重键"}
     */
    @PostMapping("/topics/{topic}/messages")
    public Map<String, Object> publish(@PathVariable String topic,
                                       @RequestBody(required = false) Map<String, String> body) {
        String content = body == null ? "" : body.getOrDefault("body", "");
        String dedupKey = body == null ? null : body.get("dedupKey");
        Object[] result = queueService.publish(topic, content, dedupKey);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("messageId", result[0]);
        resp.put("deduplicated", !((Boolean) result[1]));
        resp.put("created", result[1]);
        return resp;
    }

    /** 消费：从组里领取一条消息（带租约），无消息返回 null。 */
    @PostMapping("/groups/{group}/poll")
    public DeliveryView poll(@PathVariable String group,
                             @RequestParam(defaultValue = "default-consumer") String consumer) {
        return queueService.poll(group, consumer);
    }

    /** 确认成功。 */
    @PostMapping("/deliveries/{deliveryId}/ack")
    public Map<String, Object> ack(@PathVariable long deliveryId,
                                   @RequestParam(defaultValue = "default-consumer") String consumer) {
        return Map.of("deliveryId", deliveryId, "acknowledged",
                queueService.ack(deliveryId, consumer));
    }

    /** 拒绝（失败），可带 error 原因；服务端按退避策略重试或转入死信。 */
    @PostMapping("/deliveries/{deliveryId}/reject")
    public Map<String, Object> reject(@PathVariable long deliveryId,
                                      @RequestParam(defaultValue = "default-consumer") String consumer,
                                      @RequestBody(required = false) Map<String, String> body) {
        String error = body == null ? null : body.get("error");
        return Map.of("deliveryId", deliveryId, "rejected",
                queueService.reject(deliveryId, consumer, error));
    }

    /** 查看单条投递状态。 */
    @GetMapping("/deliveries/{deliveryId}")
    public DeliveryView delivery(@PathVariable long deliveryId) {
        return queueService.getDelivery(deliveryId);
    }

    /** 按组查看死信。 */
    @GetMapping("/groups/{group}/dead")
    public List<DeliveryView> dead(@PathVariable String group) {
        return queueService.listDead(group);
    }

    /** 手动重放死信。不绕过去重规则与重试上限。 */
    @PostMapping("/deliveries/{deliveryId}/replay")
    public Map<String, Object> replay(@PathVariable long deliveryId) {
        return Map.of("deliveryId", deliveryId, "replayed",
                queueService.replayDead(deliveryId));
    }

    /** 按组查询各类状态数量（含 total，用于校验数量守恒）。 */
    @GetMapping("/groups/{group}/stats")
    public Map<String, Object> stats(@PathVariable String group) {
        return queueService.stats(group);
    }

    /** 查看当前生效配置。 */
    @GetMapping("/config")
    public Map<String, Object> config() {
        return Map.of(
                "maxRetries", properties.getMaxRetries(),
                "backoffBaseMs", properties.getBackoff().getBaseMs(),
                "backoffMultiplier", properties.getBackoff().getMultiplier(),
                "backoffCapMs", properties.getBackoff().getCapMs(),
                "leaseMs", properties.getLeaseMs(),
                "sweepIntervalMs", properties.getSweepIntervalMs(),
                "instanceId", queueService.getInstanceId());
    }
}
