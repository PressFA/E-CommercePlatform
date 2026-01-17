package by.pressf.shoppingcartms.dto;

import java.util.UUID;

public record CreateCartRequest(UUID userId,
                                UUID productId,
                                Integer quantity) {
}
