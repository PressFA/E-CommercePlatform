package by.pressf.core.dto.orchestration.commands.user;

import java.math.BigDecimal;
import java.util.UUID;

public record CancelUserBalanceDebitCommand(UUID orderId,
                                            UUID userId,
                                            String username,
                                            BigDecimal amount) {
}
