package com.rahul.paymentservice.controller;

import com.rahul.paymentservice.dto.PaymentRequest;
import com.rahul.paymentservice.entity.Payment;
import com.rahul.paymentservice.service.PaymentService;
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
    public ResponseEntity<Payment> makePayment(@Valid @RequestBody PaymentRequest request) {
        Payment payment = paymentService.makePayment(request.getOrderId(), request.getAmount());
        return ResponseEntity.status(HttpStatus.CREATED).body(payment);
    }

    @GetMapping("/{id}")
    public Payment getPaymentById(@PathVariable Long id) {
        return paymentService.getPaymentById(id);
    }
}