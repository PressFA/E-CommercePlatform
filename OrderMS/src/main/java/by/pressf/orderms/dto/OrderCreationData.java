package by.pressf.orderms.dto;

import java.util.UUID;

public record OrderCreationData(UUID userId,
                                UUID productId,
                                Integer quantity) {
}
