package by.pressf.core.dto.orchestration.events.order;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record OrderCreatedEvent(UUID orderId,
                                UUID userId,
                                String username,
                                UUID productId,
                                Integer quantity) {
    public OrderCreatedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(productId, "productId must not be null");
        Objects.requireNonNull(quantity, "quantity must not be null");
    }
}
