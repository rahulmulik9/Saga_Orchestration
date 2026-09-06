package com.rahul.orderservice.client;

import com.rahul.orderservice.client.dto.DeductStockRequest;
import com.rahul.orderservice.client.dto.ProductResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "inventory-service", url = "http://localhost:8082")
public interface InventoryClient {

    @GetMapping("/products/{id}")
    ProductResponse getProduct(@PathVariable("id") Long id);

    @PutMapping("/products/{id}/deduct")
    ProductResponse deductStock(@PathVariable("id") Long id, @RequestBody DeductStockRequest request);
}