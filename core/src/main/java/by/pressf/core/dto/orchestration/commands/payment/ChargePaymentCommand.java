package by.pressf.core.dto.orchestration.commands.payment;

import org.jspecify.annotations.NullMarked;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

@NullMarked
public record ChargePaymentCommand(UUID orderId,
                                   UUID userId,
                                   String username,
                                   BigDecimal amount) {
    public ChargePaymentCommand {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
    }
}
