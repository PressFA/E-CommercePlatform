package by.pressf.orderms.unit.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.CriticalAuditSagaHandler;
import by.pressf.orderms.saga.listener.CriticalAuditSagaListener;
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
public class CriticalAuditSagaListenerUnitTests {
    private @Mock CriticalAuditSagaHandler handler;
    private @InjectMocks CriticalAuditSagaListener listener;

    @ParameterizedTest @MethodSource("orderRejectionEventProvider")
    void handleOrderRejectionFailed_Success_ReturnsNormally(OrderRejectionFailedEvent event,
                                                            String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleOrderRejectionFailedEvent(any(OrderRejectionFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderRejectionFailedEvent(any(OrderRejectionFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectionEventProvider")
    void handleOrderRejectionFailed_Duplicate_SwallowsException(OrderRejectionFailedEvent event,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleOrderRejectionFailedEvent(any(OrderRejectionFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderRejectionFailedEvent(any(OrderRejectionFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectionEventProvider")
    void handleOrderRejectionFailed_DataAccessError_ThrowsNotRetryable(OrderRejectionFailedEvent event,
                                                                       String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleOrderRejectionFailedEvent(any(OrderRejectionFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderRejectionFailedEvent(any(OrderRejectionFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("productCancelEventProvider")
    void handleProductReservationCancelFailed_Success_ReturnsNormally(ProductReservationCancelFailedEvent event,
                                                                      String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleProductReservationCancelFailedEvent(any(ProductReservationCancelFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationCancelFailedEvent(any(ProductReservationCancelFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productCancelEventProvider")
    void handleProductReservationCancelFailed_Duplicate_SwallowsException(ProductReservationCancelFailedEvent event,
                                                                          String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleProductReservationCancelFailedEvent(any(ProductReservationCancelFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationCancelFailedEvent(any(ProductReservationCancelFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productCancelEventProvider")
    void handleProductReservationCancelFailed_DataAccessError_ThrowsNotRetryable(ProductReservationCancelFailedEvent event,
                                                                                 String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleProductReservationCancelFailedEvent(any(ProductReservationCancelFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationCancelFailedEvent(any(ProductReservationCancelFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("paymentRefundEventProvider")
    void handlePaymentRefundFailed_Success_ReturnsNormally(PaymentRefundFailedEvent event,
                                                           String messageId) {
        // Arrange
        doNothing().when(handler)
                .handlePaymentRefundFailedEvent(any(PaymentRefundFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentRefundFailedEvent(any(PaymentRefundFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundEventProvider")
    void handlePaymentRefundFailed_Duplicate_SwallowsException(PaymentRefundFailedEvent event,
                                                               String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handlePaymentRefundFailedEvent(any(PaymentRefundFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentRefundFailedEvent(any(PaymentRefundFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundEventProvider")
    void handlePaymentRefundFailed_DataAccessError_ThrowsNotRetryable(PaymentRefundFailedEvent event,
                                                                      String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handlePaymentRefundFailedEvent(any(PaymentRefundFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentRefundFailedEvent(any(PaymentRefundFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("userCancelEventProvider")
    void handleUserBalanceDebitCancelFailed_Success_ReturnsNormally(UserBalanceDebitCancelFailedEvent event,
                                                                    String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleUserBalanceDebitCancelFailedEvent(any(UserBalanceDebitCancelFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitCancelFailedEvent(any(UserBalanceDebitCancelFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userCancelEventProvider")
    void handleUserBalanceDebitCancelFailed_Duplicate_SwallowsException(UserBalanceDebitCancelFailedEvent event,
                                                                        String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleUserBalanceDebitCancelFailedEvent(any(UserBalanceDebitCancelFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitCancelFailedEvent(any(UserBalanceDebitCancelFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userCancelEventProvider")
    void handleUserBalanceDebitCancelFailed_DataAccessError_ThrowsNotRetryable(UserBalanceDebitCancelFailedEvent event,
                                                                               String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleUserBalanceDebitCancelFailedEvent(any(UserBalanceDebitCancelFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitCancelFailedEvent(any(UserBalanceDebitCancelFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    private static Stream<Arguments> orderRejectionEventProvider() {
        return Stream.of(
                Arguments.of(
                        new OrderRejectionFailedEvent(UUID.randomUUID(), "testUser"), "msg-1")
        );
    }

    private static Stream<Arguments> productCancelEventProvider() {
        return Stream.of(
                Arguments.of(
                        new ProductReservationCancelFailedEvent(UUID.randomUUID(), "testUser"),
                        "msg-2")
        );
    }

    private static Stream<Arguments> paymentRefundEventProvider() {
        return Stream.of(
                Arguments.of(
                        new PaymentRefundFailedEvent(UUID.randomUUID(), "testUser"), "msg-3")
        );
    }

    private static Stream<Arguments> userCancelEventProvider() {
        return Stream.of(
                Arguments.of(
                        new UserBalanceDebitCancelFailedEvent(UUID.randomUUID(), "testUser"),
                        "msg-4")
        );
    }
}
