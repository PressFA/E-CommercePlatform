package by.pressf.core.dto.events.payment;

import java.util.UUID;

public record PaymentChargeFailedEvent(UUID orderId) {
}
