package com.rahul.sagaorchestratorservice.repository;

import com.rahul.sagaorchestratorservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    boolean existsByOrderIdAndEventType(Long orderId, String eventType);
}