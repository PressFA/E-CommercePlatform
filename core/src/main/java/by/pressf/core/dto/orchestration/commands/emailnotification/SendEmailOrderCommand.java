package by.pressf.core.dto.orchestration.commands.emailnotification;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;

@NullMarked
public record SendEmailOrderCommand(String email,
                                    String subject,
                                    String body) {
    public SendEmailOrderCommand {
        Objects.requireNonNull(email, "email must not be null");
        Objects.requireNonNull(subject, "subject must not be null");
        Objects.requireNonNull(body, "body must not be null");
    }
}
