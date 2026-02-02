package by.pressf.orderms.dto;

import java.util.UUID;

public record OrderCreationData(UUID userId,
                                String username,
                                UUID productId,
                                Integer quantity) {
}
