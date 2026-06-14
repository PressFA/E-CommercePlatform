package by.pressf.userms.unit.kafka.listener;

import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.handler.UserCommandsHandler;
import by.pressf.userms.kafka.listener.UserCommandsListener;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class UserCommandsListenerUnitTests {
    private @Mock Environment env;
    private @Mock UserCommandsHandler handler;
    private @InjectMocks UserCommandsListener listener;

    private static final String SUCCESS_ERROR_TOPIC = "errors-successful-events-topic";
    private static final String COMPENSATE_ERROR_TOPIC = "errors-compensating-events-topic";

    @ParameterizedTest @MethodSource("handleDebitCommand")
    void handleDebitCommand_Success_ReturnsNormally(DebitUserBalanceCommand command,
                                                    String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitCommand")
    void handleDebitCommand_DuplicateMessage_SwallowsException(DebitUserBalanceCommand command,
                                                               String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitCommand")
    void handleDebitCommand_OptimisticLockingFailure_ThrowsRetryableException(DebitUserBalanceCommand command,
                                                                              String messageId) {
        // Arrange
        doThrow(mock(OptimisticLockingFailureException.class)).when(handler)
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());
        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());

        assertInstanceOf(OptimisticLockingFailureException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceDebitFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleDebitCommand")
    void handleDebitCommand_UserNotFound_ThrowsNotRetryableException(DebitUserBalanceCommand command,
                                                                     String messageId) {
        // Arrange
        doThrow(mock(UserNotFoundException.class)).when(handler)
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());
        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());

        assertInstanceOf(UserNotFoundException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceDebitFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleDebitCommand")
    void handleDebitCommand_InsufficientBalance_ThrowsNotRetryableException(DebitUserBalanceCommand command,
                                                                            String messageId) {
        // Arrange
        doThrow(mock(InsufficientBalanceException.class)).when(handler)
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());
        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());

        assertInstanceOf(InsufficientBalanceException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceDebitFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleDebitCommand")
    void handleDebitCommand_DataAccessException_ThrowsNotRetryableException(DebitUserBalanceCommand command,
                                                                            String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());
        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleDebitUserBalanceCommand(any(DebitUserBalanceCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceDebitFailedEvent.class, exception.getValue());
    }

    private static Stream<Arguments> handleDebitCommand() {
        UUID uuid = UUID.randomUUID();

        return Stream.of(
                Arguments.of(new DebitUserBalanceCommand(uuid, uuid, "user@test.com",
                        BigDecimal.TEN), "msg-test-123")
        );
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_Success_ReturnsNormally(CancelUserBalanceDebitCommand command,
                                                     String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_DuplicateMessage_SwallowsException(CancelUserBalanceDebitCommand command,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_OptimisticLockingFailure_ThrowsRetryableException(CancelUserBalanceDebitCommand command,
                                                                               String messageId) {
        // Arrange
        doThrow(mock(OptimisticLockingFailureException.class)).when(handler)
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());
        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());

        assertInstanceOf(OptimisticLockingFailureException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceDebitCancelFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_UserNotFound_ThrowsNotRetryableException(CancelUserBalanceDebitCommand command,
                                                                      String messageId) {
        // Arrange
        doThrow(mock(UserNotFoundException.class)).when(handler)
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());
        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());

        assertInstanceOf(UserNotFoundException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceDebitCancelFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_DataAccessException_ThrowsNotRetryableException(CancelUserBalanceDebitCommand command,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());
        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelUserBalanceDebitCommand(any(CancelUserBalanceDebitCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceDebitCancelFailedEvent.class, exception.getValue());
    }

    private static Stream<Arguments> handleCancelCommand() {
        UUID uuid = UUID.randomUUID();

        return Stream.of(
                Arguments.of(new CancelUserBalanceDebitCommand(uuid, uuid, "user@test.com",
                                BigDecimal.TEN), "msg-test-123")
        );
    }
}
