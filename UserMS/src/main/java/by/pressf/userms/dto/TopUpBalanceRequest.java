package by.pressf.userms.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

public record TopUpBalanceRequest(
        @NotNull(message = "The userId field is empty!")
        UUID userId,
        @NotNull(message = "The amount field is empty!")
        @DecimalMin(value = "5", message = "The minimum amount you can add to your account is $5")
        @DecimalMax(value = "10000", message = "The maximum amount you can add to your account is $10000")
        BigDecimal amount) {
}
