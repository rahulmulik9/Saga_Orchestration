package com.rahul.sagaorchestratorservice.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.rahul.sagaorchestratorservice.dto.inventory.KafkaTopics;
import com.rahul.sagaorchestratorservice.dto.inventory.OrderItemEvent;
import com.rahul.sagaorchestratorservice.dto.inventory.ReleaseInventoryCommand;
import com.rahul.sagaorchestratorservice.dto.payment.PaymentFailed;
import com.rahul.sagaorchestratorservice.entity.SagaState;
import com.rahul.sagaorchestratorservice.entity.SagaStatus;
import com.rahul.sagaorchestratorservice.repository.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentFailedListener {

    private final SagaStateRepository sagaStateRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = com.rahul.sagaorchestratorservice.dto.payment.KafkaTopics.PAYMENT_FAILED,
            containerFactory = "paymentFailedContainerFactory"
    )
    public void handle(PaymentFailed event) throws Exception {
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

        ReleaseInventoryCommand command = new ReleaseInventoryCommand(event.getOrderId(), items);
        kafkaTemplate.send(KafkaTopics.INVENTORY_RELEASE, command);

        log.info("Saga COMPENSATING for orderId={}, reason={}, sent ReleaseInventoryCommand",
                event.getOrderId(), event.getReason());
    }
}