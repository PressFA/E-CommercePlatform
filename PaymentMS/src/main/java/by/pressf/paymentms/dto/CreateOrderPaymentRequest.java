package by.pressf.paymentms.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateOrderPaymentRequest(String idempotencyKey,
                                        UUID orderId,
                                        UUID userId,
                                        BigDecimal amount) {
}
