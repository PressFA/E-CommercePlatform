package by.pressf.core.dto.orchestration.commands.product;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record CancelProductReservationCommand(UUID orderId,
                                              String username) {
    public CancelProductReservationCommand {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
