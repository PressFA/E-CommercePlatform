package by.pressf.core.dto.orchestration.events.order;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record OrderCompletedEvent(UUID orderId,
                                  String username) {
    public OrderCompletedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
