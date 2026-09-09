package com.rahul.orderservice.listener;

import com.rahul.orderservice.dto.sagaDto.ConfirmOrderCommand;
import com.rahul.orderservice.dto.sagaDto.KafkaTopics;
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
public class ConfirmOrderListener {

    private final OrderRepository orderRepository;

    @KafkaListener(topics = KafkaTopics.ORDER_CONFIRM, containerFactory = "confirmOrderContainerFactory")
    public void handle(ConfirmOrderCommand command) {
        Order order = orderRepository.findById(command.getOrderId()).orElse(null);
        if (order == null) {
            log.warn("No Order found for orderId={}, ignoring ConfirmOrderCommand", command.getOrderId());
            return;
        }

        order.setStatus(OrderStatus.COMPLETED);
        orderRepository.save(order);

        log.info("Order {} marked COMPLETED", command.getOrderId());
    }
}