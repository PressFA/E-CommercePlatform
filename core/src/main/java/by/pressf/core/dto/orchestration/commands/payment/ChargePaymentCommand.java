package by.pressf.core.dto.orchestration.commands.payment;

import java.math.BigDecimal;
import java.util.UUID;

public record ChargePaymentCommand(UUID orderId,
                                   UUID userId,
                                   String username,
                                   BigDecimal amount) {
}
