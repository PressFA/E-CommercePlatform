package by.pressf.core.dto.orchestration.events.cart;

import java.util.UUID;

public record CreateOrderShoppingCart(UUID userId,
                                      String username,
                                      UUID productId,
                                      Integer quantity) {
}
