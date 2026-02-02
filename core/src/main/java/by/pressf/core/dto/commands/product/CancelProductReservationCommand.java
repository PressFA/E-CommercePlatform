package by.pressf.core.dto.commands.product;

import java.util.UUID;

public record CancelProductReservationCommand(UUID orderId,
                                              String username) {
}
