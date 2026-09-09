package com.rahul.orderservice.listener;

import com.rahul.orderservice.dto.sagaDto.ConfirmOrderCommand;
import com.rahul.orderservice.dto.sagaDto.KafkaTopics;
import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.entity.OrderStatus;
import com.rahul.orderservice.entity.ProcessedEvent;
import com.rahul.orderservice.repository.OrderRepository;
import com.rahul.orderservice.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class ConfirmOrderListener {

    private static final String EVENT_TYPE = "CONFIRM_ORDER";

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = KafkaTopics.ORDER_CONFIRM, containerFactory = "confirmOrderContainerFactory")
    public void handle(ConfirmOrderCommand command) {
        if (processedEventRepository.existsByOrderIdAndEventType(command.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", command.getOrderId(), EVENT_TYPE);
            return;
        }

        Order order = orderRepository.findById(command.getOrderId()).orElse(null);
        if (order == null) {
            log.warn("No Order found for orderId={}, ignoring ConfirmOrderCommand", command.getOrderId());
            return;
        }

        order.setStatus(OrderStatus.COMPLETED);
        orderRepository.save(order);
        processedEventRepository.save(new ProcessedEvent(null, command.getOrderId(), EVENT_TYPE, LocalDateTime.now()));

        log.info("Order {} marked COMPLETED", command.getOrderId());
    }
}