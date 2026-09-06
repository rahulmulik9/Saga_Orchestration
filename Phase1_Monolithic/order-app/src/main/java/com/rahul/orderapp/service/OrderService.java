package com.rahul.orderapp.service;

import com.rahul.orderapp.dto.OrderItemRequest;
import com.rahul.orderapp.dto.PlaceOrderRequest;
import com.rahul.orderapp.entity.Order;
import com.rahul.orderapp.entity.OrderItem;
import com.rahul.orderapp.entity.OrderStatus;
import com.rahul.orderapp.entity.Payment;
import com.rahul.orderapp.entity.PaymentStatus;
import com.rahul.orderapp.entity.Product;
import com.rahul.orderapp.exception.InsufficientStockException;
import com.rahul.orderapp.exception.PaymentFailedException;
import com.rahul.orderapp.repository.OrderRepository;
import com.rahul.orderapp.repository.ProductRepository;
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
    private final ProductRepository productRepository;
    private final PaymentService paymentService;

    public Order createOrder(Order order) {
        order.setStatus(OrderStatus.PENDING);
        order.setCreatedAt(LocalDateTime.now());
        order.getItems().forEach(item -> item.setOrder(order));
        return orderRepository.save(order);
    }

    public Order getOrderById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Order not found with id: " + id));
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    @Transactional
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
}