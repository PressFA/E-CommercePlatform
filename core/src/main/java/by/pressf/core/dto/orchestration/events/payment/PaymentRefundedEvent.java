package by.pressf.core.dto.orchestration.events.payment;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record PaymentRefundedEvent(UUID orderId,
                                   String username) {
    public PaymentRefundedEvent {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
