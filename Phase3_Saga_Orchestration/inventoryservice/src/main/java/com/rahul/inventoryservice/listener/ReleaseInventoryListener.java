package com.rahul.inventoryservice.listener;

import com.rahul.inventoryservice.dto.sagaDto.InventoryReleased;
import com.rahul.inventoryservice.dto.sagaDto.KafkaTopics;
import com.rahul.inventoryservice.dto.sagaDto.OrderItemEvent;
import com.rahul.inventoryservice.dto.sagaDto.ReleaseInventoryCommand;
import com.rahul.inventoryservice.entity.Product;
import com.rahul.inventoryservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReleaseInventoryListener {

    private final ProductRepository productRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.INVENTORY_RELEASE, containerFactory = "releaseInventoryContainerFactory")
    @Transactional
    public void handle(ReleaseInventoryCommand command) {
        log.info("Received ReleaseInventoryCommand for orderId={}", command.getOrderId());

        for (OrderItemEvent item : command.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product == null) {
                log.warn("Product {} not found while releasing stock for orderId={}",
                        item.getProductId(), command.getOrderId());
                continue;
            }
            product.setQuantity(product.getQuantity() + item.getQuantity());
            productRepository.save(product);
        }

        kafkaTemplate.send(KafkaTopics.INVENTORY_RELEASED, new InventoryReleased(command.getOrderId()));

        log.info("Stock released for orderId={}, published InventoryReleased", command.getOrderId());
    }
}