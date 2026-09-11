package com.rahul.sagaorchestratorservice.dto.inventory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ReleaseInventoryCommand {
    private Long orderId;
    private List<OrderItemEvent> items;
}