package by.pressf.userms.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record DebitUserBalanceRequest(UUID userId,
                                      BigDecimal amount) {
}
