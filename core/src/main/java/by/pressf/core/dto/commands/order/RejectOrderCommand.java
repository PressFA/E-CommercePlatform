package by.pressf.core.dto.commands.order;

import java.util.UUID;

public record RejectOrderCommand(UUID orderId,
                                 String username) {
}
