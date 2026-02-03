package by.pressf.paymentms.dto;

import java.math.BigDecimal;

public record StripeOrderPaymentDto(String idempotencyKey,
                                    BigDecimal amount) {
}
