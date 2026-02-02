package by.pressf.core.dto.orchestration.events.payment;

import java.util.UUID;

public record PaymentRefundFailedEvent(UUID orderId,
                                       String username) {
}
