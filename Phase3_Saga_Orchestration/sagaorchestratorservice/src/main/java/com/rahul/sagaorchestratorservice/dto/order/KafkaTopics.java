package com.rahul.sagaorchestratorservice.dto.order;

public final class KafkaTopics {
    private KafkaTopics() {}
    public static final String ORDER_CREATED = "order.created.event";
    public static final String ORDER_CONFIRM = "order.confirm.command";
    public static final String ORDER_FAILED = "order.failed.command";
}