package by.pressf.core.dto.orchestration.events.cart;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.UUID;

@NullMarked
public record CreateOrderShoppingCart(UUID userId,
                                      String username,
                                      UUID productId,
                                      Integer quantity) {
    public CreateOrderShoppingCart {
        Objects.requireNonNull(userId, "userId must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(productId, "productId must not be null");
        Objects.requireNonNull(quantity, "quantity must not be null");
    }
}
