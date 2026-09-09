package com.rahul.inventoryservice.listener;

import com.rahul.inventoryservice.dto.sagaDto.InventoryRejected;
import com.rahul.inventoryservice.dto.sagaDto.InventoryReserved;
import com.rahul.inventoryservice.dto.sagaDto.KafkaTopics;
import com.rahul.inventoryservice.dto.sagaDto.OrderItemEvent;
import com.rahul.inventoryservice.dto.sagaDto.ReserveInventoryCommand;
import com.rahul.inventoryservice.entity.Product;
import com.rahul.inventoryservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReserveInventoryListener {

    private final ProductRepository productRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.INVENTORY_RESERVE, containerFactory = "reserveInventoryContainerFactory")
    @Transactional
    public void handle(ReserveInventoryCommand command) {
        log.info("Received ReserveInventoryCommand for orderId={}", command.getOrderId());

        // Step 1: check every item has enough stock
        for (OrderItemEvent item : command.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product == null || product.getQuantity() < item.getQuantity()) {
                log.info("Reservation FAILED for orderId={}, productId={}",
                        command.getOrderId(), item.getProductId());

                kafkaTemplate.send(KafkaTopics.INVENTORY_REJECTED,
                        new InventoryRejected(command.getOrderId(), "Insufficient stock"));
                return;
            }
        }

        // Step 2: all good, deduct stock and total the amount
        BigDecimal totalAmount = BigDecimal.ZERO;
        for (OrderItemEvent item : command.getItems()) {
            Product product = productRepository.findById(item.getProductId()).get();
            product.setQuantity(product.getQuantity() - item.getQuantity());
            productRepository.save(product);
            totalAmount = totalAmount.add(product.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
        }

        log.info("Reservation SUCCESS for orderId={}, totalAmount={}", command.getOrderId(), totalAmount);

        kafkaTemplate.send(KafkaTopics.INVENTORY_RESERVED,
                new InventoryReserved(command.getOrderId(), totalAmount));
    }
}