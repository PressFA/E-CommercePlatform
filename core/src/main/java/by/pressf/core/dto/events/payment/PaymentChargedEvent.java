package by.pressf.core.dto.events.payment;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentChargedEvent(UUID orderId,
                                  UUID userId,
                                  BigDecimal amount) {
}
