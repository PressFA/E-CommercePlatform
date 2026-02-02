package by.pressf.core.dto.orchestration.commands.product;

import java.util.UUID;

public record ReserveProductCommand(UUID orderId,
                                    UUID productId,
                                    UUID userId,
                                    String username,
                                    Integer quantity) {
}
