package by.pressf.core.dto.events;

import java.util.UUID;

public record UserBalanceDebitCancelFailedEvent(UUID orderID) {
}
