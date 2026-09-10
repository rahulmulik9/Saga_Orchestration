package com.rahul.sagaorchestratorservice.repository;

import com.rahul.sagaorchestratorservice.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {
    List<OutboxEvent> findByPublishedFalse();
}