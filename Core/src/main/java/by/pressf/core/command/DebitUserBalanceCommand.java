package by.pressf.core.command;

import java.math.BigDecimal;
import java.util.UUID;

public record DebitUserBalanceCommand(UUID orderId,
                                      UUID userId,
                                      BigDecimal amount) {
}
