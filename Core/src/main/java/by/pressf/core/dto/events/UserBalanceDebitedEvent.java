package by.pressf.core.dto.events;

import java.math.BigDecimal;
import java.util.UUID;

public record UserBalanceDebitedEvent(UUID orderId,
                                      UUID userId,
                                      BigDecimal amount) {
}
