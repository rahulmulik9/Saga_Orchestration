package com.rahul.orderservice.listener;

import com.rahul.orderservice.dto.sagaDto.KafkaTopics;
import com.rahul.orderservice.dto.sagaDto.OrderFailedCommand;
import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.entity.OrderStatus;
import com.rahul.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderFailedListener {

    private final OrderRepository orderRepository;

    @KafkaListener(topics = KafkaTopics.ORDER_FAILED, containerFactory = "orderFailedContainerFactory")
    public void handle(OrderFailedCommand command) {
        Order order = orderRepository.findById(command.getOrderId()).orElse(null);
        if (order == null) {
            log.warn("No Order found for orderId={}, ignoring OrderFailedCommand", command.getOrderId());
            return;
        }

        order.setStatus(OrderStatus.FAILED);
        orderRepository.save(order);

        log.info("Order {} marked FAILED, reason={}", command.getOrderId(), command.getReason());
    }
}