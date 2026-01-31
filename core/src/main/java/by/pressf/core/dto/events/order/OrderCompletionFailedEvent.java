package by.pressf.core.dto.events.order;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderCompletionFailedEvent(UUID orderId,
                                         UUID userId,
                                         BigDecimal amount) {
}
