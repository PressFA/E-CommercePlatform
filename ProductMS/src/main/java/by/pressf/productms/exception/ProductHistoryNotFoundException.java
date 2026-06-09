package by.pressf.productms.exception;

import java.util.UUID;

public class ProductHistoryNotFoundException extends RuntimeException {
    public ProductHistoryNotFoundException(UUID orderId) {
        super("The product history record with the order ID " + orderId + " was not found");
    }
}
