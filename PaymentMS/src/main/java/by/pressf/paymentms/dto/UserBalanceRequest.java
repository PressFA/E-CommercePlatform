package by.pressf.paymentms.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record UserBalanceRequest(String idempotencyKey,
                                 UUID userId,
                                 BigDecimal amount) {
}
