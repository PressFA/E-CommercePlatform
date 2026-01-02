package by.pressf.core.exceptions;

public class NotRetryableException extends RuntimeException {
    public NotRetryableException(String message) { super(message); }

    public NotRetryableException(Throwable cause) { super(cause); }
}
