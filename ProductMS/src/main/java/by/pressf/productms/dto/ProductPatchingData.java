package by.pressf.productms.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record ProductPatchingData(UUID productId,
                                  Integer quantity,
                                  BigDecimal price) {
}
