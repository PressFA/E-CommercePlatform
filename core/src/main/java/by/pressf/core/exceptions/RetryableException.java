package by.pressf.core.exceptions;

import java.util.UUID;

public class RetryableException extends EventException {

    public RetryableException(Throwable cause) {
        super(cause);
    }

    public RetryableException(Throwable cause, String topicName, UUID key, Object value) {
        super(cause, topicName, key, value);
    }
}
