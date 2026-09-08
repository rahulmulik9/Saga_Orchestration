package com.rahul.sagaorchestratorservice.entity;

public enum SagaStatus {
    STARTED,
    INVENTORY_RESERVED,
    PAYMENT_COMPLETED,
    COMPLETED,
    COMPENSATING,
    FAILED
}