package by.pressf.orderms.dto.incoming;

import jakarta.validation.constraints.*;

import java.util.UUID;

public record CreateOrderRequest(
        @NotNull(message = "The userId field is empty!")
        UUID userId,
        @NotBlank(message = "The username(email) field is empty!")
        @Email(message = "Invalid email address!")
        String username,
        @NotNull(message = "The productId field is empty!")
        UUID productId,
        @NotNull(message = "The quantity field is empty!")
        @Min(value = 1, message = "The minimum order quantity is 1 piece!")
        @Max(value = 10, message = "The maximum order quantity is 10 pieces!")
        Integer quantity) {
}
