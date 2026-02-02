package by.pressf.core.dto.orchestration.events.user;

import java.util.UUID;

public record UserBalanceDebitCancelFailedEvent(UUID orderId,
                                                String username) {
}
