package com.rahul.inventoryservice.dto.sagaDto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderItemEvent {
    private Long productId;
    private int quantity;
}