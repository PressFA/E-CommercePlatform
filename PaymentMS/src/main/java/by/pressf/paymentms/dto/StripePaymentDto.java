package by.pressf.paymentms.dto;

import java.math.BigDecimal;

public record StripePaymentDto(String orderId,
                               BigDecimal amount) {
}
