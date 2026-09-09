package com.rahul.paymentservice.repository;

import com.rahul.paymentservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    boolean existsByOrderIdAndEventType(Long orderId, String eventType);
}