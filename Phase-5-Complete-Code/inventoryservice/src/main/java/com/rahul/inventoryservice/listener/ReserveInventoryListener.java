
package com.rahul.inventoryservice.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rahul.inventoryservice.dto.sagaDto.InventoryRejected;
import com.rahul.inventoryservice.dto.sagaDto.InventoryReserved;
import com.rahul.inventoryservice.dto.sagaDto.KafkaTopics;
import com.rahul.inventoryservice.dto.sagaDto.OrderItemEvent;
import com.rahul.inventoryservice.dto.sagaDto.ReserveInventoryCommand;
import com.rahul.inventoryservice.entity.OutboxEvent;
import com.rahul.inventoryservice.entity.Product;
import com.rahul.inventoryservice.entity.ProcessedEvent;
import com.rahul.inventoryservice.repository.OutboxEventRepository;
import com.rahul.inventoryservice.repository.ProcessedEventRepository;
import com.rahul.inventoryservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReserveInventoryListener {

    private static final String EVENT_TYPE = "RESERVE_INVENTORY";

    private final ProductRepository productRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopics.INVENTORY_RESERVE, containerFactory = "reserveInventoryContainerFactory")
    @Transactional
    public void handle(ReserveInventoryCommand command) {
        if (processedEventRepository.existsByOrderIdAndEventType(command.getOrderId(), EVENT_TYPE)) {
            log.info("orderId={} already processed for {}, skipping (idempotent)", command.getOrderId(), EVENT_TYPE);
            return;
        }

        log.info("Received ReserveInventoryCommand for orderId={}", command.getOrderId());

        for (OrderItemEvent item : command.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product == null || product.getQuantity() < item.getQuantity()) {
                log.info("Reservation FAILED for orderId={}, productId={}",
                        command.getOrderId(), item.getProductId());

                saveOutbox(KafkaTopics.INVENTORY_REJECTED,
                        new InventoryRejected(command.getOrderId(), "Insufficient stock"));
                processedEventRepository.save(new ProcessedEvent(null, command.getOrderId(), EVENT_TYPE, LocalDateTime.now()));
                return;
            }
        }

        BigDecimal totalAmount = BigDecimal.ZERO;
        for (OrderItemEvent item : command.getItems()) {
            Product product = productRepository.findById(item.getProductId()).get();
            product.setQuantity(product.getQuantity() - item.getQuantity());
            productRepository.save(product);
            totalAmount = totalAmount.add(product.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
        }

        log.info("Reservation SUCCESS for orderId={}, totalAmount={}", command.getOrderId(), totalAmount);

        saveOutbox(KafkaTopics.INVENTORY_RESERVED, new InventoryReserved(command.getOrderId(), totalAmount));
        processedEventRepository.save(new ProcessedEvent(null, command.getOrderId(), EVENT_TYPE, LocalDateTime.now()));
    }

    private void saveOutbox(String topic, Object payload) {
        try {
            OutboxEvent outboxEvent = new OutboxEvent(null, topic,
                    objectMapper.writeValueAsString(payload), false, LocalDateTime.now(), null);
            outboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize outbox payload for topic " + topic, e);
        }
    }
}















//package com.rahul.inventoryservice.listener;
//
//import com.rahul.inventoryservice.dto.sagaDto.InventoryRejected;
//import com.rahul.inventoryservice.dto.sagaDto.InventoryReserved;
//import com.rahul.inventoryservice.dto.sagaDto.KafkaTopics;
//import com.rahul.inventoryservice.dto.sagaDto.OrderItemEvent;
//import com.rahul.inventoryservice.dto.sagaDto.ReserveInventoryCommand;
//import com.rahul.inventoryservice.entity.Product;
//import com.rahul.inventoryservice.entity.ProcessedEvent;
//import com.rahul.inventoryservice.repository.ProcessedEventRepository;
//import com.rahul.inventoryservice.repository.ProductRepository;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Component;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.math.BigDecimal;
//import java.time.LocalDateTime;
//
//@Component
//@RequiredArgsConstructor
//@Slf4j
//public class ReserveInventoryListener {
//
//    private static final String EVENT_TYPE = "RESERVE_INVENTORY";
//
//    private final ProductRepository productRepository;
//    private final ProcessedEventRepository processedEventRepository;
//    private final KafkaTemplate<String, Object> kafkaTemplate;
//
//    @KafkaListener(topics = KafkaTopics.INVENTORY_RESERVE, containerFactory = "reserveInventoryContainerFactory")
//    @Transactional
//    public void handle(ReserveInventoryCommand command) {
//        if (processedEventRepository.existsByOrderIdAndEventType(command.getOrderId(), EVENT_TYPE)) {
//            log.info("orderId={} already processed for {}, skipping (idempotent)", command.getOrderId(), EVENT_TYPE);
//            return;
//        }
//
//        log.info("Received ReserveInventoryCommand for orderId={}", command.getOrderId());
//
//        for (OrderItemEvent item : command.getItems()) {
//            Product product = productRepository.findById(item.getProductId()).orElse(null);
//            if (product == null || product.getQuantity() < item.getQuantity()) {
//                log.info("Reservation FAILED for orderId={}, productId={}",
//                        command.getOrderId(), item.getProductId());
//
//                kafkaTemplate.send(KafkaTopics.INVENTORY_REJECTED,
//                        new InventoryRejected(command.getOrderId(), "Insufficient stock"));
//                processedEventRepository.save(new ProcessedEvent(null, command.getOrderId(), EVENT_TYPE, LocalDateTime.now()));
//                return;
//            }
//        }
//
//        BigDecimal totalAmount = BigDecimal.ZERO;
//        for (OrderItemEvent item : command.getItems()) {
//            Product product = productRepository.findById(item.getProductId()).get();
//            product.setQuantity(product.getQuantity() - item.getQuantity());
//            productRepository.save(product);
//            totalAmount = totalAmount.add(product.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
//        }
//
//        log.info("Reservation SUCCESS for orderId={}, totalAmount={}", command.getOrderId(), totalAmount);
//
//        kafkaTemplate.send(KafkaTopics.INVENTORY_RESERVED,
//                new InventoryReserved(command.getOrderId(), totalAmount));
//        processedEventRepository.save(new ProcessedEvent(null, command.getOrderId(), EVENT_TYPE, LocalDateTime.now()));
//    }
//}