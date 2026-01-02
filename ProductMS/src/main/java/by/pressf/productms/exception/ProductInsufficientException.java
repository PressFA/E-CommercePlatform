package by.pressf.productms.exception;

import java.util.UUID;

public class ProductInsufficientException extends RuntimeException {
    public ProductInsufficientException(UUID productId, UUID orderId) {
        super("Product " + productId + " has insufficient quantity in the stock for order " + orderId);
    }
}
