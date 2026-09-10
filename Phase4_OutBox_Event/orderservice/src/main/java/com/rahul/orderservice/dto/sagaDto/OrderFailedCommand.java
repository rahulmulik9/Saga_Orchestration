package com.rahul.orderservice.dto.sagaDto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderFailedCommand {
    private Long orderId;
    private String reason;
}