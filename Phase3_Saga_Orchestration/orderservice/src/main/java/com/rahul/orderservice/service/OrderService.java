package com.rahul.orderservice.service;

import com.rahul.orderservice.dto.PlaceOrderRequest;
import com.rahul.orderservice.dto.sagaDto.KafkaTopics;
import com.rahul.orderservice.dto.sagaDto.OrderCreated;
import com.rahul.orderservice.dto.sagaDto.OrderItemEvent;
import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.entity.OrderItem;
import com.rahul.orderservice.entity.OrderStatus;
import com.rahul.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderRepository orderRepository;

    public Order getOrderById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Order not found with id: " + id));
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    /// older feign client code
//    @Transactional
//    public Order placeOrder(PlaceOrderRequest request) {
//        Order order = new Order();
//        order.setStatus(OrderStatus.PENDING);
//        order.setCreatedAt(LocalDateTime.now());
//
//        // PHASE 1: Validate every item BEFORE deducting anything.
//        // Prevents a mid-loop failure from leaving earlier items' stock
//        // already deducted with nothing to roll it back.
//        for (OrderItemRequest line : request.getItems()) {
//            ProductResponse product;
//            try {
//                product = inventoryClient.getProduct(line.getProductId());
//            } catch (FeignException.NotFound ex) {
//                throw new NoSuchElementException("Product not found with id: " + line.getProductId());
//            }
//
//            if (product.getQuantity() < line.getQuantity()) {
//                throw new InsufficientStockException(
//                        "Insufficient stock for product id " + line.getProductId()
//                                + ": requested " + line.getQuantity() + ", available " + product.getQuantity());
//            }
//        }
//
//        BigDecimal total = BigDecimal.ZERO;
//
//        // PHASE 2: All items validated - now safe to actually deduct.
//        for (OrderItemRequest line : request.getItems()) {
//            ProductResponse product = inventoryClient.deductStock(
//                    line.getProductId(),
//                    new DeductStockRequest(line.getQuantity()));
//
//            OrderItem item = new OrderItem();
//            item.setProductId(product.getId());
//            item.setQuantity(line.getQuantity());
//            item.setPrice(product.getPrice());
//            item.setOrder(order);
//            order.getItems().add(item);
//
//            total = total.add(product.getPrice().multiply(BigDecimal.valueOf(line.getQuantity())));
//        }
//
//        // Persist as PENDING first so we have a real orderId for payment-service
//        Order savedOrder = orderRepository.save(order);
//
//        // Charge payment via payment-service
//        PaymentResponse payment = paymentClient.makePayment(new PaymentRequest(savedOrder.getId(), total));
//        if (!"SUCCESS".equals(payment.getStatus())) {
//            savedOrder.setStatus(OrderStatus.FAILED);
//            orderRepository.save(savedOrder);
//            throw new PaymentFailedException("Payment failed for amount: " + total);
//        }
//
//        savedOrder.setStatus(OrderStatus.COMPLETED);
//        return orderRepository.save(savedOrder);
//    }


    // No stock check, no payment call here anymore - orderservice no longer
    // decides the order's outcome. It just records intent as PENDING and
    // hands control to the saga orchestrator (wired in from Step 3 onward).
    @Transactional
    public Order placeOrder(PlaceOrderRequest request) {
        Order order = new Order();
        order.setStatus(OrderStatus.PENDING);
        order.setCreatedAt(LocalDateTime.now());

        request.getItems().forEach(line -> {
            OrderItem item = new OrderItem();
            item.setProductId(line.getProductId());
            item.setQuantity(line.getQuantity());
            item.setOrder(order);
            order.getItems().add(item);
        });

        Order savedOrder = orderRepository.save(order);

        List<OrderItemEvent> items = savedOrder.getItems().stream()
                .map(i -> new OrderItemEvent(i.getProductId(), i.getQuantity()))
                .collect(Collectors.toList());

        OrderCreated event = new OrderCreated(savedOrder.getId(), items);
        kafkaTemplate.send(KafkaTopics.ORDER_CREATED, event);

        return savedOrder;
    }
}