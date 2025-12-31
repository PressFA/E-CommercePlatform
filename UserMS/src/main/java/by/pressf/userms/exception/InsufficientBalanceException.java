package by.pressf.userms.exception;

import java.math.BigDecimal;
import java.util.UUID;

public class InsufficientBalanceException extends RuntimeException {
    public InsufficientBalanceException(UUID userId, BigDecimal balance, BigDecimal amount) {
        super("The user with the id " + userId + " does not have enough funds " + balance
                + " to pay for the order in the amount " + amount);
    }
}
