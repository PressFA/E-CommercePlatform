package by.pressf.core.dto.commands;

import java.util.UUID;

public record ReserveProductCommand(UUID orderId,
                                    UUID productId,
                                    UUID userId,
                                    Integer quantity) {
}
