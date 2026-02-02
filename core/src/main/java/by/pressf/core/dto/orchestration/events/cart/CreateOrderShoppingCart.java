package by.pressf.core.dto.orchestration.events.cart;

import java.util.UUID;

public record CreateOrderShoppingCart(UUID userId,
                                      UUID productId,
                                      Integer quantity) {
}
