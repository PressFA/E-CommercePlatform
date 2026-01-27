package by.pressf.core.dto.events;

import java.util.UUID;

public record CreateOrderShoppingCart(UUID userId,
                                      UUID productId,
                                      Integer quantity) {
}
