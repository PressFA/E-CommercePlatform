package by.pressf.paymentms.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record StripeUserPaymentDto(UUID userId,
                                   BigDecimal amount) {
}
