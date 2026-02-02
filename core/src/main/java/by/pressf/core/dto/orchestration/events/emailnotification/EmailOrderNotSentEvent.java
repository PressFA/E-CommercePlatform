package by.pressf.core.dto.orchestration.events.emailnotification;

import java.util.UUID;

public record EmailOrderNotSentEvent(UUID orderId) {
}
