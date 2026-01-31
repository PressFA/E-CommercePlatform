package by.pressf.core.dto.events.product;

import java.util.UUID;

public record ProductReservationCancelFailedEvent(UUID orderId) {
}
