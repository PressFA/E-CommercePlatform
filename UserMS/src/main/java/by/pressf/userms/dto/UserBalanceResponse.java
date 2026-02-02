package by.pressf.userms.dto;

import java.math.BigDecimal;

public record UserBalanceResponse(String name,
                                  BigDecimal balance) {
}
