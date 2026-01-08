package by.pressf.core.dto.events;

import java.util.UUID;

public record ProductReservationCancelFailedEvent(UUID orderId) {
}
