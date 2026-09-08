package com.rahul.paymentservice.listener;

import com.rahul.paymentservice.dto.sagaDto.KafkaTopics;
import com.rahul.paymentservice.dto.sagaDto.ProcessPaymentCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ProcessPaymentListener {

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_PROCESS,
            containerFactory = "processPaymentContainerFactory"
    )
    public void handle(ProcessPaymentCommand command) {
        log.info("Received ProcessPaymentCommand for orderId={}, amount={}",
                command.getOrderId(), command.getAmount());
        // charge simulation logic comes in Step 5.2
    }
}