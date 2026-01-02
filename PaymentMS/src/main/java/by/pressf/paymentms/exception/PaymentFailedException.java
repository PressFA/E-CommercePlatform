package by.pressf.paymentms.exception;

import lombok.Getter;

@Getter
public class PaymentFailedException extends RuntimeException {
    private final int statusCode;

    public PaymentFailedException(String message, int statusCode) {
        super(message + " Status code: " + statusCode);
        this.statusCode = statusCode;
    }
}
