package by.pressf.paymentms.dto;

import java.math.BigDecimal;

public record StripeUserPaymentDto(String idempotencyKey,
                                   BigDecimal amount) {
}
