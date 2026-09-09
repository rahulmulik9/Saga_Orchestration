package com.rahul.sagaorchestratorservice.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rahul.sagaorchestratorservice.dto.inventory.KafkaTopics;
import com.rahul.sagaorchestratorservice.dto.inventory.ReserveInventoryCommand;
import com.rahul.sagaorchestratorservice.dto.order.OrderCreated;
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
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedListener {

    private final SagaStateRepository sagaStateRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = com.rahul.sagaorchestratorservice.dto.order.KafkaTopics.ORDER_CREATED,
            containerFactory = "orderCreatedContainerFactory"
    )
    public void handle(OrderCreated event) throws Exception {
        List<com.rahul.sagaorchestratorservice.dto.inventory.OrderItemEvent> items = event.getItems().stream()
                .map(i -> new com.rahul.sagaorchestratorservice.dto.inventory.OrderItemEvent(
                        i.getProductId(), i.getQuantity()))
                .collect(Collectors.toList());

        SagaState sagaState = new SagaState();
        sagaState.setOrderId(event.getOrderId());
        sagaState.setStatus(SagaStatus.STARTED);
        sagaState.setCreatedAt(LocalDateTime.now());
        sagaState.setUpdatedAt(LocalDateTime.now());
        sagaState.setItemsJson(objectMapper.writeValueAsString(items));
        sagaStateRepository.save(sagaState);

        ReserveInventoryCommand command = new ReserveInventoryCommand(event.getOrderId(), items);
        kafkaTemplate.send(KafkaTopics.INVENTORY_RESERVE, command);

        log.info("Saga STARTED for orderId={}, sent ReserveInventoryCommand", event.getOrderId());
    }
}