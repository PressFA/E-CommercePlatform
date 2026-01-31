package by.pressf.shoppingcartms.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record QuantityChangeCart(
        @NotNull(message = "The id field is empty!")
        UUID id,
        @NotNull(message = "The quantity field is empty!")
        @Min(value = -1, message = "You can only take away 1 item at a time!")
        @Max(value = 1, message = "You can only add 1 item at a time!")
        Integer quantity) {
}
