package by.pressf.core.dto.orchestration.events.order;

import java.util.UUID;

public record OrderCompletedEvent(UUID orderId,
                                  String username) {
}
