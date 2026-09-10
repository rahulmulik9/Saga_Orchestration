package com.rahul.sagaorchestratorservice.dto.inventory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InventoryRejected {
    private Long orderId;
    private String reason;
}