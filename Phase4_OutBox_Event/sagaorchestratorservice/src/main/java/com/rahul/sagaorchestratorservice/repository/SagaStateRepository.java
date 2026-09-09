package com.rahul.sagaorchestratorservice.repository;

import com.rahul.sagaorchestratorservice.entity.SagaState;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SagaStateRepository extends JpaRepository<SagaState, Long> {
    Optional<SagaState> findByOrderId(Long orderId);
}