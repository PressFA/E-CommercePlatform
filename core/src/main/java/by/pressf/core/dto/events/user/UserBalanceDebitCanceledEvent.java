package by.pressf.core.dto.events.user;

import java.util.UUID;

public record UserBalanceDebitCanceledEvent(UUID orderID,
                                            String username) {
}
