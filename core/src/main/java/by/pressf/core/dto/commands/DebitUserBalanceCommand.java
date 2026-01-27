package by.pressf.core.dto.commands;

import java.math.BigDecimal;
import java.util.UUID;

public record DebitUserBalanceCommand(UUID orderId,
                                      UUID userId,
                                      BigDecimal amount) {
}
