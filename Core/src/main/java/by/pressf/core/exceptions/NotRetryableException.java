package by.pressf.core.exceptions;

import java.util.UUID;

public class NotRetryableException extends EventException {

    public NotRetryableException(Throwable cause) {
        super(cause);
    }

    public NotRetryableException(Throwable cause, String topicName, UUID key, Object value) {
        super(cause, topicName, key, value);
    }
}
