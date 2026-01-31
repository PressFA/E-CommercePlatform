package by.pressf.core.dto.events.product;

import java.util.UUID;

public record ProductReservationFailedEvent(UUID orderId) {
}
