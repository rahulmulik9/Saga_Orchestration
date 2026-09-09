package com.rahul.paymentservice.listener;

import com.rahul.paymentservice.dto.sagaDto.KafkaTopics;
import com.rahul.paymentservice.dto.sagaDto.PaymentCompleted;
import com.rahul.paymentservice.dto.sagaDto.PaymentFailed;
import com.rahul.paymentservice.dto.sagaDto.ProcessPaymentCommand;
import com.rahul.paymentservice.entity.Payment;
import com.rahul.paymentservice.entity.PaymentStatus;
import com.rahul.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProcessPaymentListener {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.PAYMENT_PROCESS, containerFactory = "processPaymentContainerFactory")
    public void handle(ProcessPaymentCommand command) {
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
    }
}