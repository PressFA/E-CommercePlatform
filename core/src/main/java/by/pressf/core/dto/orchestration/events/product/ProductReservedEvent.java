package by.pressf.core.dto.orchestration.events.product;

import java.math.BigDecimal;
import java.util.UUID;

public record ProductReservedEvent(UUID orderId,
                                   UUID userId,
                                   String username,
                                   BigDecimal amount) {
}
