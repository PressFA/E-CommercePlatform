package by.pressf.productms.dto.incoming;

import jakarta.validation.constraints.*;

import java.math.BigDecimal;

public record CreateProductRequest(
        @NotBlank(message = "The name field is empty!")
        @Size(min = 3, max = 50, message = "Product name limit: from 3 to 50 characters!")
        String name,
        @NotNull(message = "The quantity field is empty!")
        @Min(value = 1, message = "The minimum quantity of products for registration in the service is 1 piece!")
        @Max(value = 100, message = "The maximum quantity of products for registration in the service is 100 pieces!")
        Integer quantity,
        @NotNull(message = "The price field is empty!")
        @DecimalMin(value = "1", message = "The minimum price of the product is 1 dollar!")
        @DecimalMax(value = "50000", message = "The maximum price of the product is 50,000 dollars!")
        BigDecimal price) {
}
