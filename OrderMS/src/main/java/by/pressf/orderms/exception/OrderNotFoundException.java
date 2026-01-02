package by.pressf.orderms.exception;

import java.util.UUID;

public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(UUID id) {
        super("Order with id " + id + " not found");
    }
}
