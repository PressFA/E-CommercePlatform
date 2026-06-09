package by.pressf.productms.dto.incoming;

import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.util.UUID;

public record PatchProductRequest(
        @NotNull(message = "The productId field is empty!")
        UUID productId,
        @Min(value = 1, message = "You can add a minimum 1 item!")
        @Max(value = 100, message = "You can add a maximum of 100 items!")
        Integer quantity,
        @DecimalMin(value = "1", message = "The minimum price of the product is 1 dollar!")
        @DecimalMax(value = "50000", message = "The maximum price of the product is 50,000 dollars!")
        BigDecimal price) {
}
