package by.pressf.shoppingcartms.dto.internal;

import java.util.UUID;

public record CartInfo(UUID id,
                       UUID userId,
                       UUID productId,
                       Integer quantity) {
}
