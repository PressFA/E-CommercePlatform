package by.pressf.productms.dto.internal;

import java.math.BigDecimal;
import java.util.UUID;

public record ProductData(UUID id,
                          String name,
                          Integer quantity,
                          BigDecimal price) {
}
