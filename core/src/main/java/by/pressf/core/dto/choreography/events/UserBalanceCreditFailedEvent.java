package by.pressf.core.dto.choreography.events;

import org.jspecify.annotations.NullMarked;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

@NullMarked
public record UserBalanceCreditFailedEvent(UUID userId,
                                           String email,
                                           BigDecimal amount) {
    public UserBalanceCreditFailedEvent {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(email, "email must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
    }
}
