package by.pressf.paymentms.dto;

import java.util.UUID;

public record RefundPaymentRequest(String idempotencyKey,
                                   UUID orderId) {
}
