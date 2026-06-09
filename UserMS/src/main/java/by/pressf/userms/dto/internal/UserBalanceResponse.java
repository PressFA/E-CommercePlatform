package by.pressf.userms.dto.internal;

import java.math.BigDecimal;

public record UserBalanceResponse(String name,
                                  BigDecimal balance) {
}
