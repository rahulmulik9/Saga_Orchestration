package com.rahul.sagaorchestratorservice.dto.payment;

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