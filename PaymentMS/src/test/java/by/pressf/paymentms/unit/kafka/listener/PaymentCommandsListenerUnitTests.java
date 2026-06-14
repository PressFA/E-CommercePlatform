package by.pressf.paymentms.unit.kafka.listener;

import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.exception.PaymentNotFoundByOrderIdException;
import by.pressf.paymentms.kafka.handler.PaymentCommandsHandler;
import by.pressf.paymentms.kafka.listener.PaymentCommandsListener;
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
public class PaymentCommandsListenerUnitTests {
    private @Mock Environment env;
    private @Mock PaymentCommandsHandler handler;
    private @InjectMocks PaymentCommandsListener listener;

    private static final String SUCCESS_ERROR_TOPIC = "errors-successful-events-topic";
    private static final String COMPENSATE_ERROR_TOPIC = "errors-compensating-events-topic";

    @ParameterizedTest @MethodSource("handleChargeCommand")
    void handleChargeCommand_Success_ReturnsNormally(ChargePaymentCommand command,
                                                     String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleChargeCommand")
    void handleChargeCommand_DuplicateMessage_SwallowsException(ChargePaymentCommand command,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleChargeCommand")
    void handleChargeCommand_StripeRetryable_ThrowsRetryableException(ChargePaymentCommand command,
                                                                      String messageId) {
        // Arrange
        doThrow(new PaymentFailedException("Stripe error", 400)).when(handler)
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        assertInstanceOf(PaymentFailedException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentChargeFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleChargeCommand")
    void handleChargeCommand_StripeNotRetryable_ThrowsNotRetryableException(ChargePaymentCommand command,
                                                                            String messageId) {
        // Arrange
        doThrow(new PaymentFailedException("Stripe error", 500)).when(handler)
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        assertInstanceOf(PaymentFailedException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentChargeFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleChargeCommand")
    void handleChargeCommand_PaymentNotFound_ThrowsNotRetryableException(ChargePaymentCommand command,
                                                                         String messageId) {
        // Arrange
        doThrow(mock(PaymentNotFoundByOrderIdException.class)).when(handler)
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        assertInstanceOf(PaymentNotFoundByOrderIdException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentChargeFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleChargeCommand")
    void handleChargeCommand_DataAccessException_ThrowsNotRetryableException(ChargePaymentCommand command,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleChargePaymentCommand(any(ChargePaymentCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentChargeFailedEvent.class, exception.getValue());
    }

    private static Stream<Arguments> handleChargeCommand() {
        return Stream.of(
                Arguments.of(new ChargePaymentCommand(UUID.randomUUID(), UUID.randomUUID(),
                        "testUser", BigDecimal.TEN), "msg-charge-123")
        );
    }

    @ParameterizedTest @MethodSource("handleRefundCommand")
    void handleRefundCommand_Success_ReturnsNormally(RefundPaymentCommand command,
                                                     String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleRefundCommand")
    void handleRefundCommand_DuplicateMessage_SwallowsException(RefundPaymentCommand command,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleRefundCommand")
    void handleRefundCommand_StripeRetryable_ThrowsRetryableException(RefundPaymentCommand command,
                                                                      String messageId) {
        // Arrange
        doThrow(new PaymentFailedException("Stripe error", 400)).when(handler)
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        assertInstanceOf(PaymentFailedException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentRefundFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleRefundCommand")
    void handleRefundCommand_StripeNotRetryable_ThrowsNotRetryableException(RefundPaymentCommand command,
                                                                            String messageId) {
        // Arrange
        doThrow(new PaymentFailedException("Stripe error", 500)).when(handler)
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        assertInstanceOf(PaymentFailedException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentRefundFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleRefundCommand")
    void handleRefundCommand_PaymentNotFound_ThrowsNotRetryableException(RefundPaymentCommand command,
                                                                         String messageId) {
        // Arrange
        doThrow(mock(PaymentNotFoundByOrderIdException.class)).when(handler)
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        assertInstanceOf(PaymentNotFoundByOrderIdException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentRefundFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleRefundCommand")
    void handleRefundCommand_DataAccessException_ThrowsNotRetryableException(RefundPaymentCommand command,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRefundPaymentCommand(any(RefundPaymentCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(PaymentRefundFailedEvent.class, exception.getValue());
    }

    private static Stream<Arguments> handleRefundCommand() {
        return Stream.of(
                Arguments.of(new RefundPaymentCommand(UUID.randomUUID(), "testUser"),
                        "msg-refund-123")
        );
    }
}
