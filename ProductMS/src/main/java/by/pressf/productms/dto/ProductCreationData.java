package by.pressf.productms.dto;

import java.math.BigDecimal;

public record ProductCreationData(String name,
                                  Integer quantity,
                                  BigDecimal price) {
}
