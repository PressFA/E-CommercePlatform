package by.pressf.orderms.unit.saga.handler;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.saga.handler.ForwardSagaHandler;
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

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ForwardSagaHandlerUnitTests {
    private @Mock IdempotencyService idempotencyService;
    private @Mock OrderHistoryService orderHistoryService;
    private @Mock KafkaCommandPublisher kafkaCommandPublisher;
    private @InjectMocks ForwardSagaHandler handler;

    @ParameterizedTest @MethodSource("orderCreatedArgs")
    void handleOrderCreated_Success_CompletesSuccessfully(OrderCreatedEvent event,
                                                          String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendReserveProductCommand(anyString(), any(ReserveProductCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleOrderCreatedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendReserveProductCommand(anyString(), any(ReserveProductCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCreatedArgs")
    void handleOrderCreated_Duplicate_ThrowsDuplicateMessageException(OrderCreatedEvent event,
                                                                      String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleOrderCreatedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendReserveProductCommand(anyString(), any(ReserveProductCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCreatedArgs")
    void handleOrderCreated_KafkaError_ThrowsKafkaException(OrderCreatedEvent event,
                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendReserveProductCommand(anyString(), any(ReserveProductCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleOrderCreatedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendReserveProductCommand(anyString(), any(ReserveProductCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCreatedArgs")
    void handleOrderCreated_DataAccessError_ThrowsDataAccessException(OrderCreatedEvent event,
                                                                      String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendReserveProductCommand(anyString(), any(ReserveProductCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleOrderCreatedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendReserveProductCommand(anyString(), any(ReserveProductCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> orderCreatedArgs() {
        return Stream.of(
                Arguments.of(new OrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID(),
                        "sagaUser", UUID.randomUUID(), 3), "msg-fwd-001")
        );
    }

    @ParameterizedTest @MethodSource("productReservedArgs")
    void handleProductReserved_Success_CompletesSuccessfully(ProductReservedEvent event,
                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendChargePaymentCommand(anyString(), any(ChargePaymentCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleProductReservedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendChargePaymentCommand(anyString(), any(ChargePaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservedArgs")
    void handleProductReserved_Duplicate_ThrowsDuplicateMessageException(ProductReservedEvent event,
                                                                         String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleProductReservedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendChargePaymentCommand(anyString(), any(ChargePaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservedArgs")
    void handleProductReserved_KafkaError_ThrowsKafkaException(ProductReservedEvent event,
                                                               String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendChargePaymentCommand(anyString(), any(ChargePaymentCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleProductReservedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendChargePaymentCommand(anyString(), any(ChargePaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservedArgs")
    void handleProductReserved_DataAccessError_ThrowsDataAccessException(ProductReservedEvent event,
                                                                         String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendChargePaymentCommand(anyString(), any(ChargePaymentCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleProductReservedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendChargePaymentCommand(anyString(), any(ChargePaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> productReservedArgs() {
        return Stream.of(
                Arguments.of(new ProductReservedEvent(UUID.randomUUID(), UUID.randomUUID(),
                        "sagaUser", BigDecimal.TEN), "msg-fwd-002")
        );
    }

    @ParameterizedTest @MethodSource("paymentChargedArgs")
    void handlePaymentCharged_Success_CompletesSuccessfully(PaymentChargedEvent event,
                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendDebitUserBalanceCommand(anyString(), any(DebitUserBalanceCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handlePaymentChargedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendDebitUserBalanceCommand(anyString(), any(DebitUserBalanceCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargedArgs")
    void handlePaymentCharged_Duplicate_ThrowsDuplicateMessageException(PaymentChargedEvent event,
                                                                        String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handlePaymentChargedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendDebitUserBalanceCommand(anyString(), any(DebitUserBalanceCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargedArgs")
    void handlePaymentCharged_KafkaError_ThrowsKafkaException(PaymentChargedEvent event,
                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendDebitUserBalanceCommand(anyString(), any(DebitUserBalanceCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handlePaymentChargedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendDebitUserBalanceCommand(anyString(), any(DebitUserBalanceCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargedArgs")
    void handlePaymentCharged_DataAccessError_ThrowsDataAccessException(PaymentChargedEvent event,
                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendDebitUserBalanceCommand(anyString(), any(DebitUserBalanceCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handlePaymentChargedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendDebitUserBalanceCommand(anyString(), any(DebitUserBalanceCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> paymentChargedArgs() {
        return Stream.of(
                Arguments.of(new PaymentChargedEvent(UUID.randomUUID(), UUID.randomUUID(),
                        "sagaUser", BigDecimal.TEN), "msg-fwd-003")
        );
    }

    @ParameterizedTest @MethodSource("userBalanceDebitedArgs")
    void handleUserBalanceDebited_Success_CompletesSuccessfully(UserBalanceDebitedEvent event,
                                                                String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendConfirmOrderCommand(anyString(), any(ConfirmOrderCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleUserBalanceDebitedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendConfirmOrderCommand(anyString(), any(ConfirmOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitedArgs")
    void handleUserBalanceDebited_Duplicate_ThrowsDuplicateMessageException(UserBalanceDebitedEvent event,
                                                                            String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleUserBalanceDebitedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendConfirmOrderCommand(anyString(), any(ConfirmOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitedArgs")
    void handleUserBalanceDebited_KafkaError_ThrowsKafkaException(UserBalanceDebitedEvent event,
                                                                  String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendConfirmOrderCommand(anyString(), any(ConfirmOrderCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleUserBalanceDebitedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendConfirmOrderCommand(anyString(), any(ConfirmOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitedArgs")
    void handleUserBalanceDebited_DataAccessError_ThrowsDataAccessException(UserBalanceDebitedEvent event,
                                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendConfirmOrderCommand(anyString(), any(ConfirmOrderCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleUserBalanceDebitedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendConfirmOrderCommand(anyString(), any(ConfirmOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> userBalanceDebitedArgs() {
        return Stream.of(
                Arguments.of(new UserBalanceDebitedEvent(UUID.randomUUID(), UUID.randomUUID(),
                        "sagaUser", BigDecimal.TEN), "msg-fwd-004")
        );
    }

    @ParameterizedTest @MethodSource("orderCompletedArgs")
    void handleOrderCompleted_Success_CompletesSuccessfully(OrderCompletedEvent event,
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
        assertDoesNotThrow(() -> handler.handleOrderCompletedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(2))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletedArgs")
    void handleOrderCompleted_Duplicate_ThrowsDuplicateMessageException(OrderCompletedEvent event,
                                                                        String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleOrderCompletedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletedArgs")
    void handleOrderCompleted_KafkaError_ThrowsKafkaException(OrderCompletedEvent event,
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
                () -> handler.handleOrderCompletedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletedArgs")
    void handleOrderCompleted_DataAccessError_ThrowsDataAccessException(OrderCompletedEvent event,
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
                () -> handler.handleOrderCompletedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(2))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> orderCompletedArgs() {
        return Stream.of(
                Arguments.of(new OrderCompletedEvent(UUID.randomUUID(), "sagaUser"),
                        "msg-fwd-005")
        );
    }
}
