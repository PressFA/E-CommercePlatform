package by.pressf.userms.dto;

import java.math.BigDecimal;

public record UserCreationData(String name,
                               BigDecimal balance) {
}
