package by.pressf.core.dto.commands.order;

import java.math.BigDecimal;
import java.util.UUID;

public record ConfirmOrderCommand(UUID orderId,
                                  UUID userId,
                                  String username,
                                  BigDecimal amount) {
}
