package by.pressf.core.dto.orchestration.events.user;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record UserBalanceDebitFailedEvent(UUID orderId,
                                          String username) {
    public UserBalanceDebitFailedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
