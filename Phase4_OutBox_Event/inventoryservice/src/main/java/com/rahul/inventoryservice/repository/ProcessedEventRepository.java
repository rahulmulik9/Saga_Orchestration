package com.rahul.inventoryservice.repository;

import com.rahul.inventoryservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    boolean existsByOrderIdAndEventType(Long orderId, String eventType);
}