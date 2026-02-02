package by.pressf.core.dto.events.product;

import java.util.UUID;

public record ProductReservationCanceledEvent(UUID orderId,
                                              String username) {
}
