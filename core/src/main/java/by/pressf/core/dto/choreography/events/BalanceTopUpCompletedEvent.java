package by.pressf.core.dto.choreography.events;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;

@NullMarked
public record BalanceTopUpCompletedEvent(String email,
                                         String subject,
                                         String body) {
    public BalanceTopUpCompletedEvent {
        Objects.requireNonNull(email, "email must not be null");
        Objects.requireNonNull(subject, "subject must not be null");
        Objects.requireNonNull(body, "body must not be null");
    }
}
