package com.rahul.paymentservice.dto.sagaDto;

public final class KafkaTopics {
    private KafkaTopics() {}
    public static final String PAYMENT_PROCESS = "payment.process.command";
    public static final String PAYMENT_COMPLETED = "payment.completed.event";
    public static final String PAYMENT_FAILED = "payment.failed.event";
}