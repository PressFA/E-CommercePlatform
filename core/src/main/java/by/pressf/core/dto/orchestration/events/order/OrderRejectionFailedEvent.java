package by.pressf.core.dto.orchestration.events.order;

import java.util.UUID;

public record OrderRejectionFailedEvent(UUID orderId,
                                        String username) {
}