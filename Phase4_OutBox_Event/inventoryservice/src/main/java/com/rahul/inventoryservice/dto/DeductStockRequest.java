package com.rahul.inventoryservice.dto;

import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DeductStockRequest {

    @Positive(message = "quantity must be positive")
    private int quantity;
}