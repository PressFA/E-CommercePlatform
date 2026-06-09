package by.pressf.core.dto.orchestration.events.payment;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record PaymentRefundFailedEvent(UUID orderId,
                                       String username) {
    public PaymentRefundFailedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
