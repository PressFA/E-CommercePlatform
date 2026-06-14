package by.pressf.orderms.unit.saga.handler;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.saga.handler.RollbackSagaHandler;
import by.pressf.orderms.saga.publisher.KafkaCommandPublisher;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderHistoryService;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.KafkaException;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RollbackSagaHandlerUnitTests {
    private @Mock IdempotencyService idempotencyService;
    private @Mock OrderHistoryService orderHistoryService;
    private @Mock KafkaCommandPublisher kafkaCommandPublisher;
    private @InjectMocks RollbackSagaHandler handler;

    @ParameterizedTest @MethodSource("orderRejectedArgs")
    void handleOrderRejected_Success_CompletesSuccessfully(OrderRejectedEvent event,
                                                           String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleOrderRejectedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(2))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectedArgs")
    void handleOrderRejected_Duplicate_ThrowsDuplicateMessageException(OrderRejectedEvent event,
                                                                       String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleOrderRejectedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectedArgs")
    void handleOrderRejected_KafkaError_ThrowsKafkaException(OrderRejectedEvent event,
                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleOrderRejectedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectedArgs")
    void handleOrderRejected_DataAccessError_ThrowsDataAccessException(OrderRejectedEvent event,
                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleOrderRejectedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(2))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> orderRejectedArgs() {
        return Stream.of(
                Arguments.of(new OrderRejectedEvent(UUID.randomUUID(), "rollbackUser"),
                        "msg-roll-301")
        );
    }

    @ParameterizedTest @MethodSource("productReservationCanceledArgs")
    void handleProductReservationCanceled_Success_CompletesSuccessfully(ProductReservationCanceledEvent event,
                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleProductReservationCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationCanceledArgs")
    void handleProductReservationCanceled_Duplicate_ThrowsDuplicateMessageException(ProductReservationCanceledEvent event,
                                                                                    String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleProductReservationCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationCanceledArgs")
    void handleProductReservationCanceled_KafkaError_ThrowsKafkaException(ProductReservationCanceledEvent event,
                                                                          String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleProductReservationCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationCanceledArgs")
    void handleProductReservationCanceled_DataAccessError_ThrowsDataAccessException(ProductReservationCanceledEvent event,
                                                                                    String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleProductReservationCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> productReservationCanceledArgs() {
        return Stream.of(
                Arguments.of(new ProductReservationCanceledEvent(UUID.randomUUID(),
                        "rollbackUser"), "msg-roll-302")
        );
    }

    @ParameterizedTest @MethodSource("paymentRefundedArgs")
    void handlePaymentRefunded_Success_CompletesSuccessfully(PaymentRefundedEvent event,
                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handlePaymentRefundedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundedArgs")
    void handlePaymentRefunded_Duplicate_ThrowsDuplicateMessageException(PaymentRefundedEvent event,
                                                                         String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handlePaymentRefundedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundedArgs")
    void handlePaymentRefunded_KafkaError_ThrowsKafkaException(PaymentRefundedEvent event,
                                                               String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handlePaymentRefundedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundedArgs")
    void handlePaymentRefunded_DataAccessError_ThrowsDataAccessException(PaymentRefundedEvent event,
                                                                         String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handlePaymentRefundedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> paymentRefundedArgs() {
        return Stream.of(
                Arguments.of(new PaymentRefundedEvent(UUID.randomUUID(), "rollbackUser"),
                        "msg-roll-303")
        );
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCanceledArgs")
    void handleUserBalanceDebitCanceled_Success_CompletesSuccessfully(UserBalanceDebitCanceledEvent event,
                                                                      String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleUserBalanceDebitCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCanceledArgs")
    void handleUserBalanceDebitCanceled_Duplicate_ThrowsDuplicateMessageException(UserBalanceDebitCanceledEvent event,
                                                                                  String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleUserBalanceDebitCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCanceledArgs")
    void handleUserBalanceDebitCanceled_KafkaError_ThrowsKafkaException(UserBalanceDebitCanceledEvent event,
                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleUserBalanceDebitCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCanceledArgs")
    void handleUserBalanceDebitCanceled_DataAccessError_ThrowsDataAccessException(UserBalanceDebitCanceledEvent event,
                                                                                  String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleUserBalanceDebitCanceledEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> userBalanceDebitCanceledArgs() {
        return Stream.of(
                Arguments.of(new UserBalanceDebitCanceledEvent(UUID.randomUUID(),
                        "rollbackUser"), "msg-roll-304")
        );
    }
}
