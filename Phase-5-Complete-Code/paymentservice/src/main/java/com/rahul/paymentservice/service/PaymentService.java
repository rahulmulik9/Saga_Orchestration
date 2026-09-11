package com.rahul.paymentservice.service;

import com.rahul.paymentservice.entity.Payment;
import com.rahul.paymentservice.entity.PaymentStatus;
import com.rahul.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;

    /**
     * TEACHING STAND-IN — not a real payment gateway.
     * Simulation rule: any amount >= 10,000 is treated as a "declined card"
     * and fails. Everything else succeeds. This threshold is deliberately
     * simple and documented so failure paths can be triggered on demand
     * during testing (e.g. Step 1.7.d, Phase 2.7, Phase 4.8).
     */
    private static final BigDecimal FAILURE_THRESHOLD = BigDecimal.valueOf(10000);

    public Payment makePayment(Long orderId, BigDecimal amount) {
        Payment payment = new Payment();
        payment.setOrderId(orderId);
        payment.setAmount(amount);
        payment.setCreatedAt(LocalDateTime.now());

        boolean success = amount.compareTo(FAILURE_THRESHOLD) < 0;
        payment.setStatus(success ? PaymentStatus.SUCCESS : PaymentStatus.FAILED);

        return paymentRepository.save(payment);
    }

    public Payment getPaymentById(Long id) {
        return paymentRepository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("Payment not found with id: " + id));
    }
}