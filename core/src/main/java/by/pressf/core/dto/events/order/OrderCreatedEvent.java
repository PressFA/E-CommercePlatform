package by.pressf.core.dto.events.order;

import java.util.UUID;

public record OrderCreatedEvent(UUID orderId,
                                UUID userId,
                                String username,
                                UUID productId,
                                Integer quantity) {
}
