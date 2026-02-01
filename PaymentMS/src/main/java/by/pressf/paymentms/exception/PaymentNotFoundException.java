package by.pressf.paymentms.exception;

import java.util.UUID;

public class PaymentNotFoundException extends RuntimeException {
    public PaymentNotFoundException(UUID id) {
        super("The payment record with the order ID " + id + " was not found");
    }
}
