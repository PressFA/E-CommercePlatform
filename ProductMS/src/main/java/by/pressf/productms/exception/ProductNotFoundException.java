package by.pressf.productms.exception;

import java.util.UUID;

public class ProductNotFoundException extends RuntimeException {
    public ProductNotFoundException(UUID orderId) {
        super("Product with id " + orderId + " not found");
    }
}
