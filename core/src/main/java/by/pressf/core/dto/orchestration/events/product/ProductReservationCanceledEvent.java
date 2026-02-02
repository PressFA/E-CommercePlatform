package by.pressf.core.dto.orchestration.events.product;

import java.util.UUID;

public record ProductReservationCanceledEvent(UUID orderId,
                                              String username) {
}
