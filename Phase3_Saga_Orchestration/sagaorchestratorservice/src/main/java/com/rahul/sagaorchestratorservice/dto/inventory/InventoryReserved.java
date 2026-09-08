package com.rahul.sagaorchestratorservice.dto.inventory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReserved {
    private Long orderId;
    private BigDecimal totalAmount;
}