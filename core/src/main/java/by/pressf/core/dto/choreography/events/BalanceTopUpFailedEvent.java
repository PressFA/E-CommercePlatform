package by.pressf.core.dto.choreography.events;

public record BalanceTopUpFailedEvent(String email,
                                      String subject,
                                      String body) {
}
