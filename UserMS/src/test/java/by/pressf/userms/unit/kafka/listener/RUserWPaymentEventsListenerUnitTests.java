package by.pressf.userms.unit.kafka.listener;

import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.handler.UserEventsHandler;
import by.pressf.userms.kafka.listener.RUserWPaymentEventsListener;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RUserWPaymentEventsListenerUnitTests {
    private @Mock UserEventsHandler handler;
    private @InjectMocks RUserWPaymentEventsListener listener;

    @ParameterizedTest @MethodSource("handle")
    void handle_Success_ReturnsNormally(UserBalanceCreditFailedEvent command,
                                        String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(command, messageId));
        verify(handler, times(1))
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("handle")
    void handle_DuplicateMessage_SwallowsException(UserBalanceCreditFailedEvent command,
                                                   String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(command, messageId));
        verify(handler, times(1))
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("handle")
    void handle_OptimisticLockingFailure_ThrowsRetryableException(UserBalanceCreditFailedEvent command,
                                                                  String messageId) {
        // Arrange
        doThrow(mock(OptimisticLockingFailureException.class)).when(handler)
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handle(command, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        assertInstanceOf(OptimisticLockingFailureException.class, exception.getCause());
    }

    @ParameterizedTest @MethodSource("handle")
    void handle_UserNotFound_ThrowsNotRetryableException(UserBalanceCreditFailedEvent command,
                                                         String messageId) {
        // Arrange
        doThrow(mock(UserNotFoundException.class)).when(handler)
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handle(command, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        assertInstanceOf(UserNotFoundException.class, exception.getCause());
    }

    @ParameterizedTest @MethodSource("handle")
    void handle_DataAccessException_ThrowsNotRetryableException(UserBalanceCreditFailedEvent command,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handle(command, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditFailedEvent(any(UserBalanceCreditFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
    }

    private static Stream<Arguments> handle() {
        UUID uuid = UUID.randomUUID();

        return Stream.of(
                Arguments.of(new UserBalanceCreditFailedEvent(uuid, "user@test.com",
                        BigDecimal.TEN), "msg-test-123")
        );
    }
}
