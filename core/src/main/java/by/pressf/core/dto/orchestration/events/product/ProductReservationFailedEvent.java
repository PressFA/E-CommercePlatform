package by.pressf.core.dto.orchestration.events.product;

import java.util.UUID;

public record ProductReservationFailedEvent(UUID orderId,
                                            String username) {
}
