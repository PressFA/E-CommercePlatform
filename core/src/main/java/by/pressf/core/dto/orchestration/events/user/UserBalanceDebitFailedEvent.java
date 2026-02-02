package by.pressf.core.dto.orchestration.events.user;

import java.util.UUID;

public record UserBalanceDebitFailedEvent(UUID orderId,
                                          String username) {
}
