package by.pressf.shoppingcartms.dto;

import java.util.UUID;

public record CartInfo(UUID id,
                       UUID userId,
                       UUID productId,
                       Integer quantity) {
}
