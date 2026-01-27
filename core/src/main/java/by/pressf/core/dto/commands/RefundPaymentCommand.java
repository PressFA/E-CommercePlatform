package by.pressf.core.dto.commands;

import java.util.UUID;

public record RefundPaymentCommand(UUID orderId) {
}
