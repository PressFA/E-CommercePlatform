package by.pressf.paymentms.unit.kafka.handler;

import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.paymentms.dto.CreateOrderPaymentRequest;
import by.pressf.paymentms.dto.RefundPaymentRequest;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.exception.PaymentNotFoundByOrderIdException;
import by.pressf.paymentms.kafka.handler.PaymentCommandsHandler;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaymentCommandsHandlerUnitTests {
    private @Mock PaymentService paymentService;
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @Mock IdempotencyService idempotencyService;
    private @InjectMocks PaymentCommandsHandler handler;

    @ParameterizedTest @MethodSource("chargeArgs")
    void handleChargePayment_Success_CompletesSuccessfully(ChargePaymentCommand command,
                                                           String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).createOrderPayment(any(CreateOrderPaymentRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleChargePaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .createOrderPayment(any(CreateOrderPaymentRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("chargeArgs")
    void handleChargePayment_Duplicate_ThrowsDuplicateMessageException(ChargePaymentCommand command,
                                                                       String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleChargePaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, never())
                .createOrderPayment(any(CreateOrderPaymentRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("chargeArgs")
    void handleChargePayment_PaymentFailed_ThrowsPaymentFailedException(ChargePaymentCommand command,
                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(PaymentFailedException.class)).when(paymentService)
                .createOrderPayment(any(CreateOrderPaymentRequest.class));

        // Act & Assert
        assertThrows(PaymentFailedException.class,
                () -> handler.handleChargePaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .createOrderPayment(any(CreateOrderPaymentRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("chargeArgs")
    void handleChargePayment_PaymentNotFoundByOrderId_ThrowsPaymentNotFoundByOrderIdException(ChargePaymentCommand command,
                                                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(PaymentNotFoundByOrderIdException.class)).when(paymentService)
                .createOrderPayment(any(CreateOrderPaymentRequest.class));

        // Act & Assert
        assertThrows(PaymentNotFoundByOrderIdException.class,
                () -> handler.handleChargePaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .createOrderPayment(any(CreateOrderPaymentRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("chargeArgs")
    void handleChargePayment_KafkaError_ThrowsKafkaException(ChargePaymentCommand command,
                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).createOrderPayment(any(CreateOrderPaymentRequest.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleChargePaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .createOrderPayment(any(CreateOrderPaymentRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("chargeArgs")
    void handleChargePayment_DataAccessError_ThrowsDataAccessException(ChargePaymentCommand command,
                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).createOrderPayment(any(CreateOrderPaymentRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleChargePaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .createOrderPayment(any(CreateOrderPaymentRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessagePaymentChargedEvent(anyString(), any(PaymentChargedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> chargeArgs() {
        return Stream.of(
                Arguments.of(new ChargePaymentCommand(UUID.randomUUID(), UUID.randomUUID(),
                        "payerUser", BigDecimal.TEN), "msg-charge-123")
        );
    }

    @ParameterizedTest @MethodSource("refundArgs")
    void handleRefundPayment_Success_CompletesSuccessfully(RefundPaymentCommand command,
                                                           String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).refundOrderPayment(any(RefundPaymentRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleRefundPaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .refundOrderPayment(any(RefundPaymentRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("refundArgs")
    void handleRefundPayment_Duplicate_ThrowsDuplicateMessageException(RefundPaymentCommand command,
                                                                       String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleRefundPaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, never())
                .refundOrderPayment(any(RefundPaymentRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("refundArgs")
    void handleRefundPayment_PaymentFailed_ThrowsPaymentFailedException(RefundPaymentCommand command,
                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(PaymentFailedException.class)).when(paymentService)
                .refundOrderPayment(any(RefundPaymentRequest.class));

        // Act & Assert
        assertThrows(PaymentFailedException.class,
                () -> handler.handleRefundPaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .refundOrderPayment(any(RefundPaymentRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("refundArgs")
    void handleRefundPayment_PaymentNotFoundByOrderId_ThrowsPaymentNotFoundByOrderIdException(RefundPaymentCommand command,
                                                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(PaymentNotFoundByOrderIdException.class)).when(paymentService)
                .refundOrderPayment(any(RefundPaymentRequest.class));

        // Act & Assert
        assertThrows(PaymentNotFoundByOrderIdException.class,
                () -> handler.handleRefundPaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .refundOrderPayment(any(RefundPaymentRequest.class));
        verify(kafkaEventPublisher, never())
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("refundArgs")
    void handleRefundPayment_KafkaError_ThrowsKafkaException(RefundPaymentCommand command,
                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).refundOrderPayment(any(RefundPaymentRequest.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleRefundPaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .refundOrderPayment(any(RefundPaymentRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("refundArgs")
    void handleRefundPayment_DataAccessError_ThrowsDataAccessException(RefundPaymentCommand command,
                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(paymentService).refundOrderPayment(any(RefundPaymentRequest.class));
        doNothing().when(kafkaEventPublisher)
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleRefundPaymentCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(paymentService, times(1))
                .refundOrderPayment(any(RefundPaymentRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendMessagePaymentRefundedEvent(anyString(), any(PaymentRefundedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> refundArgs() {
        return Stream.of(
                Arguments.of(new RefundPaymentCommand(UUID.randomUUID(), "payerUser"),
                        "msg-refund-456")
        );
    }
}
