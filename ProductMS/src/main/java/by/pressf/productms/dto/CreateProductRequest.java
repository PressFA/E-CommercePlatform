package by.pressf.productms.dto;

import java.math.BigDecimal;

public record CreateProductRequest(String name,
                                   Integer quantity,
                                   BigDecimal price) {
}
