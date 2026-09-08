package com.rahul.sagaorchestratorservice.listener;

import com.rahul.sagaorchestratorservice.dto.inventory.InventoryRejected;
import com.rahul.sagaorchestratorservice.dto.order.KafkaTopics;
import com.rahul.sagaorchestratorservice.dto.order.OrderFailedCommand;
import com.rahul.sagaorchestratorservice.entity.SagaState;
import com.rahul.sagaorchestratorservice.entity.SagaStatus;
import com.rahul.sagaorchestratorservice.repository.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryRejectedListener {

    private final SagaStateRepository sagaStateRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = com.rahul.sagaorchestratorservice.dto.inventory.KafkaTopics.INVENTORY_REJECTED,
            containerFactory = "inventoryRejectedContainerFactory"
    )
    public void handle(InventoryRejected event) {
        SagaState sagaState = sagaStateRepository.findByOrderId(event.getOrderId()).orElse(null);
        if (sagaState == null) {
            log.warn("No SagaState found for orderId={}, ignoring InventoryRejected", event.getOrderId());
            return;
        }

        sagaState.setStatus(SagaStatus.FAILED);
        sagaState.setUpdatedAt(LocalDateTime.now());
        sagaStateRepository.save(sagaState);

        OrderFailedCommand command = new OrderFailedCommand(event.getOrderId(), event.getReason());
        kafkaTemplate.send(KafkaTopics.ORDER_FAILED, command);

        log.info("Saga FAILED for orderId={}, reason={}, notified order-service", event.getOrderId(), event.getReason());
    }
}