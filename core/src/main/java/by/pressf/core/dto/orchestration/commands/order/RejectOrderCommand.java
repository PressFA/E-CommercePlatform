package by.pressf.core.dto.orchestration.commands.order;

import java.util.UUID;

public record RejectOrderCommand(UUID orderId,
                                 String username) {
}
