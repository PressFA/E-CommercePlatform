package by.pressf.core.dto.orchestration.commands.payment;

import java.util.UUID;

public record RefundPaymentCommand(UUID orderId,
                                   String username) {
}
