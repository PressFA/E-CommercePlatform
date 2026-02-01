package by.pressf.paymentms.exception;

import java.util.UUID;

public class PaymentNotFoundByOrderIdException extends RuntimeException {
    public PaymentNotFoundByOrderIdException(UUID id) {
        super("The payment record with the order ID " + id + " was not found");
    }
}
