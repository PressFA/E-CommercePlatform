package by.pressf.core.dto.orchestration.commands.emailnotification;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record SendEmailOrderCommand(String email,
                                    String subject,
                                    String body,
                                    UUID orderId) {
    public SendEmailOrderCommand {
        Objects.requireNonNull(email, "email must not be null");
        Objects.requireNonNull(email, "subject must not be null");
        Objects.requireNonNull(email, "body must not be null");
        Objects.requireNonNull(email, "orderId must not be null");
    }
}
