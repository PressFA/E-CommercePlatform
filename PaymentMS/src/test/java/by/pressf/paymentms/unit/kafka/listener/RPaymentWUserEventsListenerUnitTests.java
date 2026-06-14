package by.pressf.paymentms.unit.kafka.listener;

import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.kafka.handler.PaymentEventsHandler;
import by.pressf.paymentms.kafka.listener.RPaymentWUserEventsListener;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RPaymentWUserEventsListenerUnitTests {
    private @Mock Environment env;
    private @Mock PaymentEventsHandler handler;
    private @InjectMocks RPaymentWUserEventsListener listener;

    private static final String TOPIC_NAME = "r-user-w-payment-topic";

    @ParameterizedTest @MethodSource("eventProvider")
    void handle_Success_ReturnsNormally(UserBalanceCreditedEvent event,
                                        String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("eventProvider")
    void handle_DuplicateMessage_SwallowsException(UserBalanceCreditedEvent event,
                                                   String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("eventProvider")
    void handle_RetryablePaymentFailed_ThrowsRetryableException(UserBalanceCreditedEvent event,
                                                                String messageId) {
        // Arrange
        doThrow(new PaymentFailedException("Retryable error", 400)).when(handler)
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        when(env.getRequiredProperty("r-user-w-payment.topic.name")).thenReturn(TOPIC_NAME);

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        assertInstanceOf(PaymentFailedException.class, exception.getCause());
        assertEquals(TOPIC_NAME, exception.getTopicName());
        assertEquals(event.userId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceCreditFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("eventProvider")
    void handle_NotRetryablePaymentFailed_ThrowsNotRetryableException(UserBalanceCreditedEvent event,
                                                                      String messageId) {
        // Arrange
        doThrow(new PaymentFailedException("Fatal error", 500)).when(handler)
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        when(env.getRequiredProperty("r-user-w-payment.topic.name")).thenReturn(TOPIC_NAME);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        assertInstanceOf(PaymentFailedException.class, exception.getCause());
        assertEquals(TOPIC_NAME, exception.getTopicName());
        assertEquals(event.userId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceCreditFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("eventProvider")
    void handle_DataAccessException_ThrowsNotRetryableException(UserBalanceCreditedEvent event,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        when(env.getRequiredProperty("r-user-w-payment.topic.name")).thenReturn(TOPIC_NAME);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceCreditedEvent(any(UserBalanceCreditedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
        assertEquals(TOPIC_NAME, exception.getTopicName());
        assertEquals(event.userId().toString(), exception.getKey());
        assertInstanceOf(UserBalanceCreditFailedEvent.class, exception.getValue());
    }

    private static Stream<Arguments> eventProvider() {
        return Stream.of(
                Arguments.of(new UserBalanceCreditedEvent(UUID.randomUUID(), "test@user.com",
                        BigDecimal.TEN), "msg-123")
        );
    }
}
