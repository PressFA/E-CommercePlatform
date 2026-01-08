package by.pressf.orderms.dto;

import java.util.UUID;

public record CreateOrderRequest(UUID userId,
                                 UUID productId,
                                 Integer quantity) {
}
