package by.pressf.core.dto.orchestration.events.user;

import java.util.UUID;

public record UserBalanceDebitCanceledEvent(UUID orderID,
                                            String username) {
}
