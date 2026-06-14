package by.pressf.orderms.unit.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.RollbackSagaHandler;
import by.pressf.orderms.saga.listener.RollbackSagaListener;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RollbackSagaListenerUnitTests {
    private @Mock RollbackSagaHandler handler;
    private @InjectMocks RollbackSagaListener listener;

    @ParameterizedTest @MethodSource("orderRejectedEventProvider")
    void handleOrderRejected_Success_ReturnsNormally(OrderRejectedEvent event,
                                                     String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleOrderRejectedEvent(any(OrderRejectedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderRejectedEvent(any(OrderRejectedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectedEventProvider")
    void handleOrderRejected_Duplicate_SwallowsException(OrderRejectedEvent event,
                                                         String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleOrderRejectedEvent(any(OrderRejectedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderRejectedEvent(any(OrderRejectedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectedEventProvider")
    void handleOrderRejected_DataAccessError_ThrowsNotRetryable(OrderRejectedEvent event,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleOrderRejectedEvent(any(OrderRejectedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderRejectedEvent(any(OrderRejectedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("productCanceledEventProvider")
    void handleProductReservationCanceled_Success_ReturnsNormally(ProductReservationCanceledEvent event,
                                                                  String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleProductReservationCanceledEvent(any(ProductReservationCanceledEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationCanceledEvent(any(ProductReservationCanceledEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productCanceledEventProvider")
    void handleProductReservationCanceled_Duplicate_SwallowsException(ProductReservationCanceledEvent event,
                                                                      String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleProductReservationCanceledEvent(any(ProductReservationCanceledEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationCanceledEvent(any(ProductReservationCanceledEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productCanceledEventProvider")
    void handleProductReservationCanceled_DataAccessError_ThrowsNotRetryable(ProductReservationCanceledEvent event,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleProductReservationCanceledEvent(any(ProductReservationCanceledEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationCanceledEvent(any(ProductReservationCanceledEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("paymentRefundedEventProvider")
    void handlePaymentRefunded_Success_ReturnsNormally(PaymentRefundedEvent event,
                                                       String messageId) {
        // Arrange
        doNothing().when(handler)
                .handlePaymentRefundedEvent(any(PaymentRefundedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentRefundedEvent(any(PaymentRefundedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundedEventProvider")
    void handlePaymentRefunded_Duplicate_SwallowsException(PaymentRefundedEvent event,
                                                           String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handlePaymentRefundedEvent(any(PaymentRefundedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentRefundedEvent(any(PaymentRefundedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundedEventProvider")
    void handlePaymentRefunded_DataAccessError_ThrowsNotRetryable(PaymentRefundedEvent event,
                                                                  String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handlePaymentRefundedEvent(any(PaymentRefundedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentRefundedEvent(any(PaymentRefundedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("userCanceledEventProvider")
    void handleUserBalanceDebitCanceled_Success_ReturnsNormally(UserBalanceDebitCanceledEvent event,
                                                                String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleUserBalanceDebitCanceledEvent(any(UserBalanceDebitCanceledEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitCanceledEvent(any(UserBalanceDebitCanceledEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userCanceledEventProvider")
    void handleUserBalanceDebitCanceled_Duplicate_SwallowsException(UserBalanceDebitCanceledEvent event,
                                                                    String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleUserBalanceDebitCanceledEvent(any(UserBalanceDebitCanceledEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitCanceledEvent(any(UserBalanceDebitCanceledEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userCanceledEventProvider")
    void handleUserBalanceDebitCanceled_DataAccessError_ThrowsNotRetryable(UserBalanceDebitCanceledEvent event,
                                                                           String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleUserBalanceDebitCanceledEvent(any(UserBalanceDebitCanceledEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitCanceledEvent(any(UserBalanceDebitCanceledEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    private static Stream<Arguments> orderRejectedEventProvider() {
        return Stream.of(
                Arguments.of(
                        new OrderRejectedEvent(UUID.randomUUID(), "testUser"), "msg-1")
        );
    }

    private static Stream<Arguments> productCanceledEventProvider() {
        return Stream.of(
                Arguments.of(
                        new ProductReservationCanceledEvent(UUID.randomUUID(), "testUser"),
                        "msg-2")
        );
    }

    private static Stream<Arguments> paymentRefundedEventProvider() {
        return Stream.of(
                Arguments.of(
                        new PaymentRefundedEvent(UUID.randomUUID(), "testUser"), "msg-3")
        );
    }

    private static Stream<Arguments> userCanceledEventProvider() {
        return Stream.of(
                Arguments.of(
                        new UserBalanceDebitCanceledEvent(UUID.randomUUID(), "testUser"),
                        "msg-4")
        );
    }
}
