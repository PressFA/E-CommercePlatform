package by.pressf.shoppingcartms.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record CreateCartRequest(
        @NotNull(message = "The userId field is empty!")
        UUID userId,
        @NotNull(message = "The productId field is empty!")
        UUID productId,
        @NotNull(message = "The quantity field is empty!")
        @Min(value = 1, message = "The minimum order quantity is 1 piece!")
        @Max(value = 10, message = "The maximum order quantity is 10 pieces!")
        Integer quantity) {
}
