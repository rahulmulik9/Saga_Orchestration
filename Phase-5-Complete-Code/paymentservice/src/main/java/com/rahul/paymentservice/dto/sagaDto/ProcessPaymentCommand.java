package com.rahul.paymentservice.dto.sagaDto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessPaymentCommand {
    private Long orderId;
    private BigDecimal amount;
}