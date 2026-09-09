package com.rahul.inventoryservice.listener;

import com.rahul.inventoryservice.dto.sagaDto.InventoryReleased;
import com.rahul.inventoryservice.dto.sagaDto.KafkaTopics;
import com.rahul.inventoryservice.dto.sagaDto.OrderItemEvent;
import com.rahul.inventoryservice.dto.sagaDto.ReleaseInventoryCommand;
import com.rahul.inventoryservice.entity.Product;
import com.rahul.inventoryservice.entity.ProcessedEvent;
import com.rahul.inventoryservice.repository.ProcessedEventRepository;
import com.rahul.inventoryservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReleaseInventoryListener {

    private static final String EVENT_TYPE = "RELEASE_INVENTORY";

    private final ProductRepository productRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.INVENTORY_RELEASE, containerFactory = "releaseInventoryContainerFactory")
    @Transactional
    public void handle(ReleaseInventoryCommand command) {
        if (processedEventRepository.existsByOrderIdAndEventType(command.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", command.getOrderId(), EVENT_TYPE);
            return;
        }

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
        processedEventRepository.save(new ProcessedEvent(null, command.getOrderId(), EVENT_TYPE, LocalDateTime.now()));

        log.info("Stock released for orderId={}, published InventoryReleased", command.getOrderId());
    }
}