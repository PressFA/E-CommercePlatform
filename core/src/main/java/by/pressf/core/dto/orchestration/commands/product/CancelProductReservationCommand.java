package by.pressf.core.dto.orchestration.commands.product;

import java.util.UUID;

public record CancelProductReservationCommand(UUID orderId,
                                              String username) {
}
