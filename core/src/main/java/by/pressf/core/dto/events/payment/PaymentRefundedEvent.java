package by.pressf.core.dto.events.payment;

import java.util.UUID;

public record PaymentRefundedEvent(UUID orderId,
                                   String username) {
}
