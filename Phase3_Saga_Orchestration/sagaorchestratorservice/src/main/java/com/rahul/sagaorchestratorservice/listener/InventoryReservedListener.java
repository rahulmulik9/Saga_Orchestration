package com.rahul.sagaorchestratorservice.listener;

import com.rahul.sagaorchestratorservice.dto.inventory.InventoryReserved;
import com.rahul.sagaorchestratorservice.dto.payment.KafkaTopics;
import com.rahul.sagaorchestratorservice.dto.payment.ProcessPaymentCommand;
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
public class InventoryReservedListener {

    private static final String EVENT_TYPE = "INVENTORY_RESERVED";
    private final ProcessedEventRepository processedEventRepository;
    private final SagaStateRepository sagaStateRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = com.rahul.sagaorchestratorservice.dto.inventory.KafkaTopics.INVENTORY_RESERVED,
            containerFactory = "inventoryReservedContainerFactory"
    )
    public void handle(InventoryReserved event) {
        if (processedEventRepository.existsByOrderIdAndEventType(event.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", event.getOrderId(), EVENT_TYPE);
            return;
        }
        SagaState sagaState = sagaStateRepository.findByOrderId(event.getOrderId()).orElse(null);
        if (sagaState == null) {
            log.warn("No SagaState found for orderId={}, ignoring InventoryReserved", event.getOrderId());
            return;
        }

        sagaState.setStatus(SagaStatus.INVENTORY_RESERVED);
        sagaState.setUpdatedAt(LocalDateTime.now());
        sagaStateRepository.save(sagaState);

        ProcessPaymentCommand command = new ProcessPaymentCommand(event.getOrderId(), event.getTotalAmount());
        kafkaTemplate.send(KafkaTopics.PAYMENT_PROCESS, command);

        log.info("Saga INVENTORY_RESERVED for orderId={}, sent ProcessPaymentCommand", event.getOrderId());
    }
}