package by.pressf.core.dto.commands.payment;

import java.util.UUID;

public record RefundPaymentCommand(UUID orderId) {
}
