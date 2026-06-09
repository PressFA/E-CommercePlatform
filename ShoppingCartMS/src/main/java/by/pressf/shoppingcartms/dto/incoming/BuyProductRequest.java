package by.pressf.shoppingcartms.dto.incoming;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record BuyProductRequest(
        @NotNull(message = "The id field is empty!")
        UUID id,
        @NotBlank(message = "The username(email) field is empty!")
        @Email(message = "Invalid email address!")
        String username) {
}
