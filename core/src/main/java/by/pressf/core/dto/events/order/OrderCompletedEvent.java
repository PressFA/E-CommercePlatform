package by.pressf.core.dto.events.order;

import java.util.UUID;

public record OrderCompletedEvent(UUID orderId,
                                  String username) {
}
