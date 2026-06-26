package by.pressf.userms.dto.internal;

import java.math.BigDecimal;
import java.util.UUID;

public record UserBalanceRequest(UUID userId,
                                 BigDecimal amount) {
}
