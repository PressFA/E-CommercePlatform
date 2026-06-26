package by.pressf.core.dto.orchestration.events.user;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record UserBalanceDebitCancelFailedEvent(UUID orderId,
                                                String username) {
    public UserBalanceDebitCancelFailedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
