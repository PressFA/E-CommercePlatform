package by.pressf.userms.unit.kafka.handler;

import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.userms.dto.internal.UserBalanceRequest;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.handler.UserCommandsHandler;
import by.pressf.userms.kafka.publisher.KafkaEventPublisher;
import by.pressf.userms.service.IdempotencyService;
import by.pressf.userms.service.UserService;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.KafkaException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class UserCommandsHandlerUnitTests {
    private @Mock UserService userService;
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @Mock IdempotencyService idempotencyService;
    private @InjectMocks UserCommandsHandler handler;

    @ParameterizedTest @MethodSource("handleDebitUserBalance")
    void handleDebitUserBalance_Success_CompletesSuccessfully(DebitUserBalanceCommand command,
                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).debitUserBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleDebitUserBalanceCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .debitUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitUserBalance")
    void handleDebitUserBalance_Duplicate_ThrowsDuplicateMessageException(DebitUserBalanceCommand command,
                                                                          String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleDebitUserBalanceCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, never())
                .debitUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitUserBalance")
    void handleDebitUserBalance_OptimisticLockingFailure_ThrowsOptimisticLockingFailureException(DebitUserBalanceCommand command,
                                                                                                 String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(OptimisticLockingFailureException.class)).when(userService)
                .debitUserBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(OptimisticLockingFailureException.class,
                () -> handler.handleDebitUserBalanceCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .debitUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitUserBalance")
    void handleDebitUserBalance_UserNotFound_ThrowsUserNotFoundException(DebitUserBalanceCommand command,
                                                                         String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(UserNotFoundException.class)).when(userService)
                .debitUserBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(UserNotFoundException.class,
                () -> handler.handleDebitUserBalanceCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .debitUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitUserBalance")
    void handleDebitUserBalance_InsufficientBalance_ThrowsInsufficientBalanceException(DebitUserBalanceCommand command,
                                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(InsufficientBalanceException.class)).when(userService)
                .debitUserBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(InsufficientBalanceException.class,
                () -> handler.handleDebitUserBalanceCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .debitUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitUserBalance")
    void handleDebitUserBalance_KafkaError_ThrowsKafkaException(DebitUserBalanceCommand command,
                                                                String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).debitUserBalance(any(UserBalanceRequest.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleDebitUserBalanceCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .debitUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleDebitUserBalance")
    void handleDebitUserBalance_DataAccessError_ThrowsDataAccessException(DebitUserBalanceCommand command,
                                                                          String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).debitUserBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleDebitUserBalanceCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .debitUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageUserBalanceDebitedEvent(anyString(), any(UserBalanceDebitedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> handleDebitUserBalance() {
        UUID uuid = UUID.randomUUID();

        return Stream.of(
                Arguments.of(new DebitUserBalanceCommand(uuid, uuid, "user@test.com",
                        BigDecimal.TEN), "msg-test-123")
        );
    }

    @ParameterizedTest @MethodSource("handleCancelUserBalanceDebit")
    void handleCancelUserBalanceDebit_Success_CompletesSuccessfully(CancelUserBalanceDebitCommand command,
                                                                    String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).creditUserBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleCancelUserBalanceDebitCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(userService, times(1)).creditUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        verify(idempotencyService, times(1)).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelUserBalanceDebit")
    void handleCancelUserBalanceDebit_Duplicate_ThrowsDuplicateMessageException(CancelUserBalanceDebitCommand command,
                                                                                String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleCancelUserBalanceDebitCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(userService, never()).creditUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelUserBalanceDebit")
    void handleCancelUserBalanceDebit_OptimisticLockingFailure_ThrowsOptimisticLockingFailureException(CancelUserBalanceDebitCommand command,
                                                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(OptimisticLockingFailureException.class)).when(userService)
                .creditUserBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(OptimisticLockingFailureException.class,
                () -> handler.handleCancelUserBalanceDebitCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(userService, times(1)).creditUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelUserBalanceDebit")
    void handleCancelUserBalanceDebit_UserNotFound_ThrowsUserNotFoundException(CancelUserBalanceDebitCommand command,
                                                                               String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(UserNotFoundException.class)).when(userService)
                .creditUserBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(UserNotFoundException.class,
                () -> handler.handleCancelUserBalanceDebitCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(userService, times(1)).creditUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelUserBalanceDebit")
    void handleCancelUserBalanceDebit_KafkaError_ThrowsKafkaException(CancelUserBalanceDebitCommand command,
                                                                      String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).creditUserBalance(any(UserBalanceRequest.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleCancelUserBalanceDebitCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(userService, times(1)).creditUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelUserBalanceDebit")
    void handleCancelUserBalanceDebit_DataAccessError_ThrowsDataAccessException(CancelUserBalanceDebitCommand command,
                                                                                String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).creditUserBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleCancelUserBalanceDebitCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(userService, times(1)).creditUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageUserBalanceDebitCanceledEvent(anyString(), any(UserBalanceDebitCanceledEvent.class));
        verify(idempotencyService, times(1)).saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> handleCancelUserBalanceDebit() {
        UUID uuid = UUID.randomUUID();

        return Stream.of(
                Arguments.of(new CancelUserBalanceDebitCommand(uuid, uuid, "user@test.com",
                        BigDecimal.TEN), "msg-test-123")
        );
    }
}
