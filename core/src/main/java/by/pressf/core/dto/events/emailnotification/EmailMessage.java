package by.pressf.core.dto.events.emailnotification;

public record EmailMessage(String email,
                           String subject,
                           String body) {
}
