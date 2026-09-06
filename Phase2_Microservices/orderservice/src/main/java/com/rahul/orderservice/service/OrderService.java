package com.rahul.orderservice.service;

import com.rahul.orderservice.dto.PlaceOrderRequest;
import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.entity.OrderStatus;
import com.rahul.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;

    public Order getOrderById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Order not found with id: " + id));
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    // TEMPORARY STUB (Step 2.4.c) — stock check and payment call removed
    // because ProductRepository/PaymentService no longer live in this
    // service. This just persists a PENDING order for now. Step 2.6
    // reconnects the real flow using Feign clients to inventory-service
    // and payment-service.
    @Transactional
    public Order placeOrder(PlaceOrderRequest request) {
        Order order = new Order();
        order.setStatus(OrderStatus.PENDING);
        order.setCreatedAt(LocalDateTime.now());

        return orderRepository.save(order);
    }

    /// ========== old method
    /*
     public Order placeOrder(PlaceOrderRequest request) {
        Order order = new Order();
        order.setStatus(OrderStatus.PENDING);
        order.setCreatedAt(LocalDateTime.now());

        BigDecimal total = BigDecimal.ZERO;

        // 1. Validate stock & deduct
        for (OrderItemRequest line : request.getItems()) {
            Product product = productRepository.findById(line.getProductId())
                    .orElseThrow(() -> new NoSuchElementException("Product not found with id: " + line.getProductId()));

            if (product.getQuantity() < line.getQuantity()) {
                throw new InsufficientStockException(
                        "Insufficient stock for product id " + product.getId()
                                + ": requested " + line.getQuantity() + ", available " + product.getQuantity());
            }

            product.setQuantity(product.getQuantity() - line.getQuantity());
            productRepository.save(product);

            OrderItem item = new OrderItem();
            item.setProductId(product.getId());
            item.setQuantity(line.getQuantity());
            item.setPrice(product.getPrice());
            item.setOrder(order);
            order.getItems().add(item);

            total = total.add(product.getPrice().multiply(BigDecimal.valueOf(line.getQuantity())));
        }

        // 2. Charge payment
        Payment payment = paymentService.charge(null, total);
        if (payment.getStatus() != PaymentStatus.SUCCESS) {
            throw new PaymentFailedException("Payment failed for amount: " + total);
        }

        // 3. Create order (COMPLETED)
        order.setStatus(OrderStatus.COMPLETED);
        Order savedOrder = orderRepository.save(order);

        payment.setOrderId(savedOrder.getId());

        return savedOrder;
    }
    */
}