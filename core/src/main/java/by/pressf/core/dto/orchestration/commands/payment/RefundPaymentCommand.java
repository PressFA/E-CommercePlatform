package by.pressf.core.dto.orchestration.commands.payment;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record RefundPaymentCommand(UUID orderId,
                                   String username) {
    public RefundPaymentCommand {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
