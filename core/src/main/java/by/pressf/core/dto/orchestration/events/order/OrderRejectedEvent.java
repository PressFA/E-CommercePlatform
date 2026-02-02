package by.pressf.core.dto.orchestration.events.order;

import java.util.UUID;

public record OrderRejectedEvent(UUID orderId,
                                 String username) {
}