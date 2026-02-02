package by.pressf.core.dto.orchestration.events.order;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderCompletionFailedEvent(UUID orderId,
                                         UUID userId,
                                         String username,
                                         BigDecimal amount) {
}
