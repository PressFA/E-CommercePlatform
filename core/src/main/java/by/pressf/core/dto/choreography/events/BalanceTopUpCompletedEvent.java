package by.pressf.core.dto.choreography.events;

public record BalanceTopUpCompletedEvent(String email,
                                         String subject,
                                         String body) {
}
