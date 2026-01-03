package by.pressf.userms.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record CreditUserBalanceRequest(UUID userId,
                                       BigDecimal amount) {
}
