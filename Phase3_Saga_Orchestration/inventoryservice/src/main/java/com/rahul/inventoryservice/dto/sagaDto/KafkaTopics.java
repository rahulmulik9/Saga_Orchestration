package com.rahul.inventoryservice.dto.sagaDto;

public final class KafkaTopics {
    private KafkaTopics() {}
    public static final String INVENTORY_RESERVE = "inventory.reserve.command";
    public static final String INVENTORY_RESERVED = "inventory.reserved.event";
    public static final String INVENTORY_REJECTED = "inventory.rejected.event";
    public static final String INVENTORY_RELEASE = "inventory.release.command";
    public static final String INVENTORY_RELEASED = "inventory.released.event";
}