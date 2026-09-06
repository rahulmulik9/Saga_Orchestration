package com.rahul.orderservice.service;

import com.rahul.orderservice.client.InventoryClient;
import com.rahul.orderservice.client.PaymentClient;
import com.rahul.orderservice.client.dto.DeductStockRequest;
import com.rahul.orderservice.client.dto.PaymentRequest;
import com.rahul.orderservice.client.dto.PaymentResponse;
import com.rahul.orderservice.client.dto.ProductResponse;
import com.rahul.orderservice.dto.OrderItemRequest;
import com.rahul.orderservice.dto.PlaceOrderRequest;
import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.entity.OrderItem;
import com.rahul.orderservice.entity.OrderStatus;
import com.rahul.orderservice.exception.InsufficientStockException;
import com.rahul.orderservice.exception.PaymentFailedException;
import com.rahul.orderservice.repository.OrderRepository;
import feign.FeignException;
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
    private final InventoryClient inventoryClient;
    private final PaymentClient paymentClient;

    public Order getOrderById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Order not found with id: " + id));
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    // NOTE: @Transactional here only wraps the local orderRepository.save()
    // call. It has NO effect on inventoryClient/paymentClient calls -
    // those are separate HTTP requests to separate databases. If payment
    // fails after stock was already deducted, nothing rolls back on the
    // inventory-service side. This is the exact gap Step 2.7 demonstrates
    // and Phase 4's Saga fixes.
    @Transactional
    public Order placeOrder(PlaceOrderRequest request) {
        Order order = new Order();
        order.setStatus(OrderStatus.PENDING);
        order.setCreatedAt(LocalDateTime.now());

        // PHASE 1: Validate every item BEFORE deducting anything.
        // Prevents a mid-loop failure from leaving earlier items' stock
        // already deducted with nothing to roll it back.
        for (OrderItemRequest line : request.getItems()) {
            ProductResponse product;
            try {
                product = inventoryClient.getProduct(line.getProductId());
            } catch (FeignException.NotFound ex) {
                throw new NoSuchElementException("Product not found with id: " + line.getProductId());
            }

            if (product.getQuantity() < line.getQuantity()) {
                throw new InsufficientStockException(
                        "Insufficient stock for product id " + line.getProductId()
                                + ": requested " + line.getQuantity() + ", available " + product.getQuantity());
            }
        }

        BigDecimal total = BigDecimal.ZERO;

        // PHASE 2: All items validated - now safe to actually deduct.
        for (OrderItemRequest line : request.getItems()) {
            ProductResponse product = inventoryClient.deductStock(
                    line.getProductId(),
                    new DeductStockRequest(line.getQuantity()));

            OrderItem item = new OrderItem();
            item.setProductId(product.getId());
            item.setQuantity(line.getQuantity());
            item.setPrice(product.getPrice());
            item.setOrder(order);
            order.getItems().add(item);

            total = total.add(product.getPrice().multiply(BigDecimal.valueOf(line.getQuantity())));
        }

        // Persist as PENDING first so we have a real orderId for payment-service
        Order savedOrder = orderRepository.save(order);

        // Charge payment via payment-service
        PaymentResponse payment = paymentClient.makePayment(new PaymentRequest(savedOrder.getId(), total));
        if (!"SUCCESS".equals(payment.getStatus())) {
            savedOrder.setStatus(OrderStatus.FAILED);
            orderRepository.save(savedOrder);
            throw new PaymentFailedException("Payment failed for amount: " + total);
        }

        savedOrder.setStatus(OrderStatus.COMPLETED);
        return orderRepository.save(savedOrder);
    }
}