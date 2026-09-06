package com.rahul.orderapp.controller;

import com.rahul.orderapp.dto.ChargeRequest;
import com.rahul.orderapp.entity.Payment;
import com.rahul.orderapp.service.PaymentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping
    public ResponseEntity<Payment> charge(@Valid @RequestBody ChargeRequest request) {
        Payment payment = paymentService.charge(request.getOrderId(), request.getAmount());
        return ResponseEntity.status(HttpStatus.CREATED).body(payment);
    }

    @GetMapping("/{id}")
    public Payment getPaymentById(@PathVariable Long id) {
        return paymentService.getPaymentById(id);
    }
}