package com.rahul.orderservice.controller;

import com.rahul.orderapp.dto.PlaceOrderRequest;
import com.rahul.orderapp.entity.Order;
import com.rahul.orderapp.service.OrderService;
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

//    @PostMapping
//    public ResponseEntity<Order> createOrder(@Valid @RequestBody Order order) {
//        Order created = orderService.createOrder(order);
//        return ResponseEntity.status(HttpStatus.CREATED).body(created);
//    }

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
}