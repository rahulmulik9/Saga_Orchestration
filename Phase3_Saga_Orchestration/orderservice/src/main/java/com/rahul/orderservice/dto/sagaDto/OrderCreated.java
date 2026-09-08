package com.rahul.orderservice.dto.sagaDto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreated{
    Long orderId;
    List<OrderItemEvent> items;
}