package by.pressf.core.dto.events;

import java.util.UUID;

public record OrderCreatedEvent(UUID orderId,
                                UUID userId,
                                UUID productId,
                                Integer quantity) {
}
