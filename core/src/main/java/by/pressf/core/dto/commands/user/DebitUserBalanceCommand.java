package by.pressf.core.dto.commands.user;

import java.math.BigDecimal;
import java.util.UUID;

public record DebitUserBalanceCommand(UUID orderId,
                                      UUID userId,
                                      BigDecimal amount) {
}
