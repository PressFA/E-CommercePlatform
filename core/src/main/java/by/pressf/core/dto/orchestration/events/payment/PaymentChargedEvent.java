package by.pressf.core.dto.orchestration.events.payment;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentChargedEvent(UUID orderId,
                                  UUID userId,
                                  String username,
                                  BigDecimal amount) {
}
