package by.pressf.productms.dto.internal;

import java.util.UUID;

public record ProductReservationRequest(UUID orderId,
                                        UUID productId,
                                        Integer quantity) {
}
