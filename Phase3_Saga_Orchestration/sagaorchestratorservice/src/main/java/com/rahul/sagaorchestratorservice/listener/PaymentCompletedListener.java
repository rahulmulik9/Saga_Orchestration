package com.rahul.sagaorchestratorservice.listener;

import com.rahul.sagaorchestratorservice.dto.order.ConfirmOrderCommand;
import com.rahul.sagaorchestratorservice.dto.order.KafkaTopics;
import com.rahul.sagaorchestratorservice.dto.payment.PaymentCompleted;
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
public class PaymentCompletedListener {
    private static final String EVENT_TYPE = "PAYMENT_COMPLETED";
    private final ProcessedEventRepository processedEventRepository;
    private final SagaStateRepository sagaStateRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = com.rahul.sagaorchestratorservice.dto.payment.KafkaTopics.PAYMENT_COMPLETED,
            containerFactory = "paymentCompletedContainerFactory"
    )
    public void handle(PaymentCompleted event) {
        if (processedEventRepository.existsByOrderIdAndEventType(event.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", event.getOrderId(), EVENT_TYPE);
            return;
        }
        SagaState sagaState = sagaStateRepository.findByOrderId(event.getOrderId()).orElse(null);
        if (sagaState == null) {
            log.warn("No SagaState found for orderId={}, ignoring PaymentCompleted", event.getOrderId());
            return;
        }

        sagaState.setStatus(SagaStatus.PAYMENT_COMPLETED);
        sagaState.setUpdatedAt(LocalDateTime.now());
        sagaStateRepository.save(sagaState);

        ConfirmOrderCommand command = new ConfirmOrderCommand(event.getOrderId());
        kafkaTemplate.send(KafkaTopics.ORDER_CONFIRM, command);
        processedEventRepository.save(new ProcessedEvent(null, event.getOrderId(), EVENT_TYPE, LocalDateTime.now()));
        log.info("Saga PAYMENT_COMPLETED for orderId={}, sent ConfirmOrderCommand", event.getOrderId());
    }
}