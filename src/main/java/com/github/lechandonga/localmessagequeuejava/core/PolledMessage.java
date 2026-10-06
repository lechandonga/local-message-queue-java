package com.github.lechandonga.localmessagequeuejava.core;

/**
 * 投递给消费者的消息。确认/拒绝时必须回传 {@code groupStateId} 与 {@code leaseToken}。
 */
public record PolledMessage(
        long groupStateId,
        long messageId,
        String topic,
        String consumerGroup,
        String consumerId,
        String payload,
        int attempts,
        String leaseToken,
        long leaseExpiresAt) {
}
