package by.pressf.core.dto.commands.product;

import java.util.UUID;

public record ReserveProductCommand(UUID orderId,
                                    UUID productId,
                                    UUID userId,
                                    Integer quantity) {
}
