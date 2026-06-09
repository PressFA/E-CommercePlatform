package by.pressf.core.dto.orchestration.commands.order;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record RejectOrderCommand(UUID orderId,
                                 String username) {
    public RejectOrderCommand {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(username, "username must not be null");
    }
}
