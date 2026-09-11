package com.rahul.orderservice.repository;

import com.rahul.orderservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    boolean existsByOrderIdAndEventType(Long orderId, String eventType);
}