package by.pressf.orderms.unit.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.ForwardSagaHandler;
import by.pressf.orderms.saga.listener.ForwardSagaListener;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ForwardSagaListenerUnitTests {
    private @Mock ForwardSagaHandler handler;
    private @InjectMocks ForwardSagaListener listener;

    @ParameterizedTest @MethodSource("orderCreatedEventProvider")
    void handleOrderCreated_Success_ReturnsNormally(OrderCreatedEvent event,
                                                    String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleOrderCreatedEvent(any(OrderCreatedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCreatedEvent(any(OrderCreatedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderCreatedEventProvider")
    void handleOrderCreated_Duplicate_SwallowsException(OrderCreatedEvent event,
                                                        String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleOrderCreatedEvent(any(OrderCreatedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCreatedEvent(any(OrderCreatedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderCreatedEventProvider")
    void handleOrderCreated_DataAccessError_ThrowsNotRetryable(OrderCreatedEvent event,
                                                               String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleOrderCreatedEvent(any(OrderCreatedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCreatedEvent(any(OrderCreatedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("productReservedEventProvider")
    void handleProductReserved_Success_ReturnsNormally(ProductReservedEvent event,
                                                       String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleProductReservedEvent(any(ProductReservedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservedEvent(any(ProductReservedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productReservedEventProvider")
    void handleProductReserved_Duplicate_SwallowsException(ProductReservedEvent event,
                                                           String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleProductReservedEvent(any(ProductReservedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservedEvent(any(ProductReservedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productReservedEventProvider")
    void handleProductReserved_DataAccessError_ThrowsNotRetryable(ProductReservedEvent event,
                                                                  String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleProductReservedEvent(any(ProductReservedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservedEvent(any(ProductReservedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("paymentChargedEventProvider")
    void handlePaymentCharged_Success_ReturnsNormally(PaymentChargedEvent event,
                                                      String messageId) {
        // Arrange
        doNothing().when(handler)
                .handlePaymentChargedEvent(any(PaymentChargedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentChargedEvent(any(PaymentChargedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargedEventProvider")
    void handlePaymentCharged_Duplicate_SwallowsException(PaymentChargedEvent event,
                                                          String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handlePaymentChargedEvent(any(PaymentChargedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentChargedEvent(any(PaymentChargedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargedEventProvider")
    void handlePaymentCharged_DataAccessError_ThrowsNotRetryable(PaymentChargedEvent event,
                                                                 String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handlePaymentChargedEvent(any(PaymentChargedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentChargedEvent(any(PaymentChargedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitedEventProvider")
    void handleUserBalanceDebited_Success_ReturnsNormally(UserBalanceDebitedEvent event,
                                                          String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleUserBalanceDebitedEvent(any(UserBalanceDebitedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitedEvent(any(UserBalanceDebitedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitedEventProvider")
    void handleUserBalanceDebited_Duplicate_SwallowsException(UserBalanceDebitedEvent event,
                                                              String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleUserBalanceDebitedEvent(any(UserBalanceDebitedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitedEvent(any(UserBalanceDebitedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitedEventProvider")
    void handleUserBalanceDebited_DataAccessError_ThrowsNotRetryable(UserBalanceDebitedEvent event,
                                                                     String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleUserBalanceDebitedEvent(any(UserBalanceDebitedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitedEvent(any(UserBalanceDebitedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("orderCompletedEventProvider")
    void handleOrderCompleted_Success_ReturnsNormally(OrderCompletedEvent event,
                                                      String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleOrderCompletedEvent(any(OrderCompletedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCompletedEvent(any(OrderCompletedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletedEventProvider")
    void handleOrderCompleted_Duplicate_SwallowsException(OrderCompletedEvent event,
                                                          String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleOrderCompletedEvent(any(OrderCompletedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCompletedEvent(any(OrderCompletedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletedEventProvider")
    void handleOrderCompleted_DataAccessError_ThrowsNotRetryable(OrderCompletedEvent event,
                                                                 String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleOrderCompletedEvent(any(OrderCompletedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCompletedEvent(any(OrderCompletedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    private static Stream<Arguments> orderCreatedEventProvider() {
        return Stream.of(
                Arguments.of(
                        new OrderCreatedEvent(UUID.randomUUID(), UUID.randomUUID(),
                                "testUser", UUID.randomUUID(), 1), "msg-1")
        );
    }

    private static Stream<Arguments> productReservedEventProvider() {
        return Stream.of(
                Arguments.of(
                        new ProductReservedEvent(UUID.randomUUID(), UUID.randomUUID(),
                                "testUser", BigDecimal.TEN), "msg-2")
        );
    }

    private static Stream<Arguments> paymentChargedEventProvider() {
        return Stream.of(
                Arguments.of(
                        new PaymentChargedEvent(UUID.randomUUID(), UUID.randomUUID(),
                                "testUser", BigDecimal.TEN), "msg-3")
        );
    }

    private static Stream<Arguments> userBalanceDebitedEventProvider() {
        return Stream.of(
                Arguments.of(
                        new UserBalanceDebitedEvent(UUID.randomUUID(), UUID.randomUUID(),
                                "testUser", BigDecimal.TEN), "msg-4")
        );
    }

    private static Stream<Arguments> orderCompletedEventProvider() {
        return Stream.of(
                Arguments.of(
                        new OrderCompletedEvent(UUID.randomUUID(), "testUser"), "msg-5")
        );
    }
}
