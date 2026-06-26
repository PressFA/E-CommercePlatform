package by.pressf.paymentms.unit.kafka.handler;

import by.pressf.core.dto.choreography.events.BalanceTopUpCompletedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.paymentms.dto.UserBalanceRequest;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.kafka.handler.PaymentEventsHandler;
import by.pressf.paymentms.kafka.publisher.KafkaEventPublisher;
import by.pressf.paymentms.service.IdempotencyService;
import by.pressf.paymentms.service.PaymentService;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.KafkaException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaymentEventsHandlerUnitTests {
    private @Mock PaymentService paymentService;
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @Mock IdempotencyService idempotencyService;
    private @InjectMocks PaymentEventsHandler handler;

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCredited_Success_CompletesSuccessfully(UserBalanceCreditedEvent event,
                                                                 String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).topUpBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleUserBalanceCreditedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .topUpBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCredited_Duplicate_ThrowsDuplicateMessageException(UserBalanceCreditedEvent event,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleUserBalanceCreditedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, never())
                .topUpBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCredited_PaymentFailed_ThrowsPaymentFailedException(UserBalanceCreditedEvent event,
                                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(PaymentFailedException.class)).when(paymentService)
                .topUpBalance(any(UserBalanceRequest.class));

        // Act & Assert
        assertThrows(PaymentFailedException.class,
                () -> handler.handleUserBalanceCreditedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .topUpBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCredited_KafkaError_ThrowsKafkaException(UserBalanceCreditedEvent event,
                                                                   String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).topUpBalance(any(UserBalanceRequest.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleUserBalanceCreditedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .topUpBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleUserBalanceCredited_DataAccessError_ThrowsDataAccessException(UserBalanceCreditedEvent event,
                                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).topUpBalance(any(UserBalanceRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleUserBalanceCreditedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .topUpBalance(any(UserBalanceRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessageBalanceTopUpCompletedEvent(anyString(), any(BalanceTopUpCompletedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(new UserBalanceCreditedEvent(UUID.randomUUID(), "user@pressf.by",
                        BigDecimal.TEN), "msg-credited-789")
        );
    }
}
