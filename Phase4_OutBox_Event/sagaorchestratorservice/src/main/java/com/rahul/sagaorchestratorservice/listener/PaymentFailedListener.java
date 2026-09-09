package com.rahul.sagaorchestratorservice.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rahul.sagaorchestratorservice.dto.inventory.KafkaTopics;
import com.rahul.sagaorchestratorservice.dto.inventory.OrderItemEvent;
import com.rahul.sagaorchestratorservice.dto.inventory.ReleaseInventoryCommand;
import com.rahul.sagaorchestratorservice.dto.payment.PaymentFailed;
import com.rahul.sagaorchestratorservice.entity.OutboxEvent;
import com.rahul.sagaorchestratorservice.entity.ProcessedEvent;
import com.rahul.sagaorchestratorservice.entity.SagaState;
import com.rahul.sagaorchestratorservice.entity.SagaStatus;
import com.rahul.sagaorchestratorservice.repository.OutboxEventRepository;
import com.rahul.sagaorchestratorservice.repository.ProcessedEventRepository;
import com.rahul.sagaorchestratorservice.repository.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentFailedListener {

    private static final String EVENT_TYPE = "PAYMENT_FAILED";

    private final SagaStateRepository sagaStateRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = com.rahul.sagaorchestratorservice.dto.payment.KafkaTopics.PAYMENT_FAILED,
            containerFactory = "paymentFailedContainerFactory"
    )
    @Transactional
    public void handle(PaymentFailed event) throws JsonProcessingException {
        if (processedEventRepository.existsByOrderIdAndEventType(event.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", event.getOrderId(), EVENT_TYPE);
            return;
        }

        SagaState sagaState = sagaStateRepository.findByOrderId(event.getOrderId()).orElse(null);
        if (sagaState == null) {
            log.warn("No SagaState found for orderId={}, ignoring PaymentFailed", event.getOrderId());
            return;
        }

        sagaState.setStatus(SagaStatus.COMPENSATING);
        sagaState.setUpdatedAt(LocalDateTime.now());
        sagaStateRepository.save(sagaState);

        List<OrderItemEvent> items = objectMapper.readValue(
                sagaState.getItemsJson(), new TypeReference<List<OrderItemEvent>>() {});

        saveOutbox(KafkaTopics.INVENTORY_RELEASE, new ReleaseInventoryCommand(event.getOrderId(), items));
        processedEventRepository.save(new ProcessedEvent(null, event.getOrderId(), EVENT_TYPE, LocalDateTime.now()));

        log.info("Saga COMPENSATING for orderId={}, reason={}, staged ReleaseInventoryCommand",
                event.getOrderId(), event.getReason());
    }

    private void saveOutbox(String topic, Object payload) {
        try {
            OutboxEvent outboxEvent = new OutboxEvent(null, topic,
                    objectMapper.writeValueAsString(payload), false, LocalDateTime.now(), null);
            outboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize outbox payload for topic " + topic, e);
        }
    }
}