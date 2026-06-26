package by.pressf.core.dto.orchestration.events.user;

import org.jspecify.annotations.NullMarked;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

@NullMarked
public record UserBalanceDebitedEvent(UUID orderId,
                                      UUID userId,
                                      String username,
                                      BigDecimal amount) {
    public UserBalanceDebitedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
    }
}
