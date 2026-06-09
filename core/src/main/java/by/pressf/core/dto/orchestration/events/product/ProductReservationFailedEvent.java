package by.pressf.core.dto.orchestration.events.product;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record ProductReservationFailedEvent(UUID orderId,
                                            String username) {
    public ProductReservationFailedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
