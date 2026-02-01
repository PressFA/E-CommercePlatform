package by.pressf.productms.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record ProductData(UUID id,
                          String name,
                          Integer quantity,
                          BigDecimal price) {
}
