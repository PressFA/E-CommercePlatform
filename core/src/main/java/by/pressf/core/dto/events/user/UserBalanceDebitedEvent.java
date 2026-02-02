package by.pressf.core.dto.events.user;

import java.math.BigDecimal;
import java.util.UUID;

public record UserBalanceDebitedEvent(UUID orderId,
                                      UUID userId,
                                      String username,
                                      BigDecimal amount) {
}
