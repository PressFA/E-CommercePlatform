package by.pressf.core.dto.events.emailnotification;

import java.util.UUID;

public record EmailOrderNotSentEvent(UUID orderId) {
}
