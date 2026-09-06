package com.rahul.orderservice.controller;

import com.rahul.orderservice.client.InventoryClient;
import com.rahul.orderservice.client.dto.ProductResponse;
import com.rahul.orderservice.dto.PlaceOrderRequest;
import com.rahul.orderservice.entity.Order;
import com.rahul.orderservice.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping("/place")
    public ResponseEntity<Order> placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        Order order = orderService.placeOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @GetMapping("/{id}")
    public Order getOrderById(@PathVariable Long id) {
        return orderService.getOrderById(id);
    }

    @GetMapping
    public List<Order> getAllOrders() {
        return orderService.getAllOrders();
    }

    // TEMPORARY - delete after confirming Feign wiring works (Step 2.5.e)
    @RestController
    @RequiredArgsConstructor
    public class FeignTestController {

        private final InventoryClient inventoryClient;

        @GetMapping("/test/product/{id}")
        public ProductResponse testGetProduct(@PathVariable Long id) {
            return inventoryClient.getProduct(id);
        }
    }
}