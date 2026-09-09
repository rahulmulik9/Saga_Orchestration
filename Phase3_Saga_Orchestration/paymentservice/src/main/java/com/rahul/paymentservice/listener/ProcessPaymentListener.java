package com.rahul.paymentservice.listener;

import com.rahul.paymentservice.dto.sagaDto.KafkaTopics;
import com.rahul.paymentservice.dto.sagaDto.PaymentCompleted;
import com.rahul.paymentservice.dto.sagaDto.PaymentFailed;
import com.rahul.paymentservice.dto.sagaDto.ProcessPaymentCommand;
import com.rahul.paymentservice.entity.Payment;
import com.rahul.paymentservice.entity.PaymentStatus;
import com.rahul.paymentservice.entity.ProcessedEvent;
import com.rahul.paymentservice.repository.ProcessedEventRepository;
import com.rahul.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProcessPaymentListener {

    private static final String EVENT_TYPE = "PROCESS_PAYMENT";

    private final PaymentService paymentService;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.PAYMENT_PROCESS, containerFactory = "processPaymentContainerFactory")
    public void handle(ProcessPaymentCommand command) {
        if (processedEventRepository.existsByOrderIdAndEventType(command.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", command.getOrderId(), EVENT_TYPE);
            return;
        }

        log.info("Received ProcessPaymentCommand for orderId={}, amount={}",
                command.getOrderId(), command.getAmount());

        Payment payment = paymentService.makePayment(command.getOrderId(), command.getAmount());

        log.info("Payment processed for orderId={} -> status={}",
                command.getOrderId(), payment.getStatus());

        if (payment.getStatus() == PaymentStatus.SUCCESS) {
            kafkaTemplate.send(KafkaTopics.PAYMENT_COMPLETED, new PaymentCompleted(command.getOrderId()));
        } else {
            kafkaTemplate.send(KafkaTopics.PAYMENT_FAILED,
                    new PaymentFailed(command.getOrderId(), "Payment declined (amount over threshold)"));
        }

        processedEventRepository.save(new ProcessedEvent(null, command.getOrderId(), EVENT_TYPE, LocalDateTime.now()));
    }
}