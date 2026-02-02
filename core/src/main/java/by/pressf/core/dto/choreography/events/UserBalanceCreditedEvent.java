package by.pressf.core.dto.choreography.events;

import java.math.BigDecimal;
import java.util.UUID;

public record UserBalanceCreditedEvent(UUID userId,
                                       String email,
                                       BigDecimal amount) {
}
