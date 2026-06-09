package by.pressf.core.dto.choreography.events;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;

@NullMarked
public record BalanceTopUpCompletedEvent(String email,
                                         String subject,
                                         String body) {
    public BalanceTopUpCompletedEvent {
        Objects.requireNonNull(email, "email must not be null");
        Objects.requireNonNull(email, "subject must not be null");
        Objects.requireNonNull(email, "body must not be null");
    }
}
