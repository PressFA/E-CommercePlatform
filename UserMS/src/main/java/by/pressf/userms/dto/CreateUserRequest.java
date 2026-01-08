package by.pressf.userms.dto;

import java.math.BigDecimal;

public record CreateUserRequest(String name,
                                BigDecimal balance) {
}
