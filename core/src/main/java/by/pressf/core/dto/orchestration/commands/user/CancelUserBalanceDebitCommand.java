package by.pressf.core.dto.orchestration.commands.user;

import org.jspecify.annotations.NullMarked;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

@NullMarked
public record CancelUserBalanceDebitCommand(UUID orderId,
                                            UUID userId,
                                            String username,
                                            BigDecimal amount) {
    public CancelUserBalanceDebitCommand {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
    }
}
