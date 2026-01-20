package by.pressf.core.dto.events;

public record EmailMessage(String email,
                           String subject,
                           String body) {
}
