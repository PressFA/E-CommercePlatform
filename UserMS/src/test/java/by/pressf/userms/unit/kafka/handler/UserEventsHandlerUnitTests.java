package by.pressf.userms.unit.kafka.handler;

import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.userms.dto.internal.UserBalanceRequest;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.handler.UserEventsHandler;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class UserEventsHandlerUnitTests {
    private @Mock UserService userService;
    private @Mock IdempotencyService idempotencyService;
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @InjectMocks UserEventsHandler handler;

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCreditFailed_Success_CompletesSuccessfully(UserBalanceCreditFailedEvent event,
                                                                     String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).cancelTopUpUserBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher).sendMessageBalanceTopUpFailedEvent(anyString(), any());
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleUserBalanceCreditFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageBalanceTopUpFailedEvent(anyString(), any());
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCreditFailed_Duplicate_ThrowsDuplicateMessageException(UserBalanceCreditFailedEvent event,
                                                                                 String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleUserBalanceCreditFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, never())
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageBalanceTopUpFailedEvent(anyString(), any());
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCreditFailed_OptimisticLockingFailure_ThrowsOptimisticLockingFailureException(UserBalanceCreditFailedEvent event,
                                                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(OptimisticLockingFailureException.class)).when(userService)
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(OptimisticLockingFailureException.class,
                () -> handler.handleUserBalanceCreditFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageBalanceTopUpFailedEvent(anyString(), any());
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCreditFailed_UserNotFound_ThrowsUserNotFoundException(UserBalanceCreditFailedEvent event,
                                                                                String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(UserNotFoundException.class)).when(userService)
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(UserNotFoundException.class,
                () -> handler.handleUserBalanceCreditFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageBalanceTopUpFailedEvent(anyString(), any());
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCreditFailed_KafkaError_ThrowsKafkaException(UserBalanceCreditFailedEvent event,
                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).cancelTopUpUserBalance(any(UserBalanceRequest.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendMessageBalanceTopUpFailedEvent(anyString(), any());

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleUserBalanceCreditFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageBalanceTopUpFailedEvent(anyString(), any());
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCreditFailed_DataAccessError_ThrowsDataAccessException(UserBalanceCreditFailedEvent event,
                                                                                 String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(userService).cancelTopUpUserBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher).sendMessageBalanceTopUpFailedEvent(anyString(), any());
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleUserBalanceCreditFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(userService, times(1))
                .cancelTopUpUserBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageBalanceTopUpFailedEvent(anyString(), any());
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(new UserBalanceCreditFailedEvent(UUID.randomUUID(), "test@mail.com",
                        BigDecimal.TEN), "msg-test-123")
        );
    }
}
