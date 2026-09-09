package com.rahul.sagaorchestratorservice.listener;

import com.rahul.sagaorchestratorservice.dto.inventory.InventoryReleased;
import com.rahul.sagaorchestratorservice.dto.order.KafkaTopics;
import com.rahul.sagaorchestratorservice.dto.order.OrderFailedCommand;
import com.rahul.sagaorchestratorservice.entity.ProcessedEvent;
import com.rahul.sagaorchestratorservice.entity.SagaState;
import com.rahul.sagaorchestratorservice.entity.SagaStatus;
import com.rahul.sagaorchestratorservice.repository.ProcessedEventRepository;
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
public class InventoryReleasedListener {

    private static final String EVENT_TYPE = "INVENTORY_RELEASED";
    private final ProcessedEventRepository processedEventRepository;
    private final SagaStateRepository sagaStateRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = com.rahul.sagaorchestratorservice.dto.inventory.KafkaTopics.INVENTORY_RELEASED,
            containerFactory = "inventoryReleasedContainerFactory"
    )
    public void handle(InventoryReleased event) {
        if (processedEventRepository.existsByOrderIdAndEventType(event.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", event.getOrderId(), EVENT_TYPE);
            return;
        }

        SagaState sagaState = sagaStateRepository.findByOrderId(event.getOrderId()).orElse(null);
        if (sagaState == null) {
            log.warn("No SagaState found for orderId={}, ignoring InventoryReleased", event.getOrderId());
            return;
        }

        sagaState.setStatus(SagaStatus.FAILED);
        sagaState.setUpdatedAt(LocalDateTime.now());
        sagaStateRepository.save(sagaState);

        OrderFailedCommand command = new OrderFailedCommand(event.getOrderId(), "Payment failed, inventory released");
        kafkaTemplate.send(KafkaTopics.ORDER_FAILED, command);
        processedEventRepository.save(new ProcessedEvent(null, event.getOrderId(), EVENT_TYPE, LocalDateTime.now()));
        log.info("Saga FAILED (compensated) for orderId={}, notified order-service", event.getOrderId());
    }
}