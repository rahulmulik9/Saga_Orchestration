package com.rahul.orderservice.client;

import com.rahul.orderservice.client.dto.PaymentResponse;
import com.rahul.orderservice.client.dto.PaymentRequest;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "payment-service", url = "http://localhost:8083")
public interface PaymentClient {

    @PostMapping("/payments")
    PaymentResponse makePayment(@RequestBody PaymentRequest request);
}