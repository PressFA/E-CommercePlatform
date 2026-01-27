package by.pressf.core.exceptions;

import lombok.Getter;

import java.util.Date;

@Getter
public class AppError extends RuntimeException {
    private final int status;
    private final String message;
    private final Date timestamp;

    public AppError(int status, String message) {
        this.status = status;
        this.message = message;
        this.timestamp = new Date();
    }
}
