package by.pressf.productms.exception;

import java.util.UUID;

public class ProductNotFoundByOrderIdException extends RuntimeException {
    public ProductNotFoundByOrderIdException(UUID orderId) {
        super("The product record with the order ID " + orderId + " was not found");
    }
}
