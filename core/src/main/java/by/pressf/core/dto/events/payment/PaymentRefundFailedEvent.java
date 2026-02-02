package by.pressf.core.dto.events.payment;

import java.util.UUID;

public record PaymentRefundFailedEvent(UUID orderId,
                                       String username) {
}
