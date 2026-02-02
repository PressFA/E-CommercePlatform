package by.pressf.core.dto.orchestration.events.product;

import java.util.UUID;

public record ProductReservationCancelFailedEvent(UUID orderId,
                                                  String username) {
}
