package com.github.lechandonga.localmessagequeuejava.queue;

public enum DeliveryStatus {
    PENDING,
    IN_FLIGHT,
    RETRYING,
    SUCCEEDED,
    DEAD
}
