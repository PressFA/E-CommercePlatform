package by.pressf.core.exceptions;

import lombok.Getter;

import java.util.UUID;

@Getter
public abstract class EventException extends RuntimeException {
    private String topicName;
    private String key;
    private Object value;
    private String messageId;

    public EventException(Throwable cause) {
        super(cause);
    }

    public EventException(Throwable cause, String topicName, UUID key, Object value) {
        super(cause);
        this.topicName = topicName;
        this.key = key.toString();
        this.value = value;
        this.messageId = UUID.randomUUID().toString();
    }
}
