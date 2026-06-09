package by.pressf.core.exceptions;

public class DuplicateMessageException extends RuntimeException {
    public DuplicateMessageException(String messageId) {
        super("Duplicate message detected: message with ID " + messageId + " has already been processed!");
    }
}
