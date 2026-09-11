package com.rahul.sagaorchestratorservice.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SagaState {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long sagaId;

    private Long orderId;

    @Enumerated(EnumType.STRING)
    private SagaStatus status;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    private String itemsJson;
}