package com.github.lechandonga.localmessagequeuejava.core;

/** 消费者组维度的消息状态。 */
public enum GroupStatus {

    /** 待投递（含重试退避到期前的排队中）。 */
    PENDING,

    /** 已投递给某个消费者，正在处理、尚未确认。 */
    PROCESSING,

    /** 消费成功，终态。 */
    SUCCEEDED,

    /** 重试次数耗尽，进入死信。 */
    DEAD
}
