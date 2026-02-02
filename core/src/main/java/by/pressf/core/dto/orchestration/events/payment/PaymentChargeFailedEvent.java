package by.pressf.core.dto.orchestration.events.payment;

import java.util.UUID;

public record PaymentChargeFailedEvent(UUID orderId,
                                       String username) {
}
