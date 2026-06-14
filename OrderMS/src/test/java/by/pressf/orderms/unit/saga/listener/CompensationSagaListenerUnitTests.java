package by.pressf.orderms.unit.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.CompensationSagaHandler;
import by.pressf.orderms.saga.listener.CompensationSagaListener;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CompensationSagaListenerUnitTests {
    private @Mock CompensationSagaHandler handler;
    private @InjectMocks CompensationSagaListener listener;

    @ParameterizedTest @MethodSource("productEventProvider")
    void handleProductReservationFailed_Success_ReturnsNormally(ProductReservationFailedEvent event,
                                                                String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleProductReservationFailedEvent(any(ProductReservationFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationFailedEvent(any(ProductReservationFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productEventProvider")
    void handleProductReservationFailed_Duplicate_SwallowsException(ProductReservationFailedEvent event,
                                                                    String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleProductReservationFailedEvent(any(ProductReservationFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationFailedEvent(any(ProductReservationFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("productEventProvider")
    void handleProductReservationFailed_DataAccessError_ThrowsNotRetryable(ProductReservationFailedEvent event,
                                                                           String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleProductReservationFailedEvent(any(ProductReservationFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleProductReservationFailedEvent(any(ProductReservationFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("paymentEventProvider")
    void handlePaymentChargeFailed_Success_ReturnsNormally(PaymentChargeFailedEvent event,
                                                           String messageId) {
        // Arrange
        doNothing().when(handler)
                .handlePaymentChargeFailedEvent(any(PaymentChargeFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentChargeFailedEvent(any(PaymentChargeFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentEventProvider")
    void handlePaymentChargeFailed_Duplicate_SwallowsException(PaymentChargeFailedEvent event,
                                                               String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handlePaymentChargeFailedEvent(any(PaymentChargeFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentChargeFailedEvent(any(PaymentChargeFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("paymentEventProvider")
    void handlePaymentChargeFailed_DataAccessError_ThrowsNotRetryable(PaymentChargeFailedEvent event,
                                                                      String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handlePaymentChargeFailedEvent(any(PaymentChargeFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handlePaymentChargeFailedEvent(any(PaymentChargeFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("userEventProvider")
    void handleUserBalanceDebitFailed_Success_ReturnsNormally(UserBalanceDebitFailedEvent event,
                                                              String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleUserBalanceDebitFailedEvent(any(UserBalanceDebitFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitFailedEvent(any(UserBalanceDebitFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userEventProvider")
    void handleUserBalanceDebitFailed_Duplicate_SwallowsException(UserBalanceDebitFailedEvent event,
                                                                  String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleUserBalanceDebitFailedEvent(any(UserBalanceDebitFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitFailedEvent(any(UserBalanceDebitFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("userEventProvider")
    void handleUserBalanceDebitFailed_DataAccessError_ThrowsNotRetryable(UserBalanceDebitFailedEvent event,
                                                                         String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleUserBalanceDebitFailedEvent(any(UserBalanceDebitFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleUserBalanceDebitFailedEvent(any(UserBalanceDebitFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("orderEventProvider")
    void handleOrderCompletionFailed_Success_ReturnsNormally(OrderCompletionFailedEvent event,
                                                             String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleOrderCompletionFailedEvent(any(OrderCompletionFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCompletionFailedEvent(any(OrderCompletionFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderEventProvider")
    void handleOrderCompletionFailed_Duplicate_SwallowsException(OrderCompletionFailedEvent event,
                                                                 String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleOrderCompletionFailedEvent(any(OrderCompletionFailedEvent.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCompletionFailedEvent(any(OrderCompletionFailedEvent.class), anyString());
    }

    @ParameterizedTest @MethodSource("orderEventProvider")
    void handleOrderCompletionFailed_DataAccessError_ThrowsNotRetryable(OrderCompletionFailedEvent event,
                                                                        String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleOrderCompletionFailedEvent(any(OrderCompletionFailedEvent.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleOrderCompletionFailedEvent(any(OrderCompletionFailedEvent.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    private static Stream<Arguments> productEventProvider() {
        return Stream.of(
                Arguments.of(
                        new ProductReservationFailedEvent(UUID.randomUUID(), "testUser"), "msg-1")
        );
    }

    private static Stream<Arguments> paymentEventProvider() {
        return Stream.of(
                Arguments.of(
                        new PaymentChargeFailedEvent(UUID.randomUUID(), "testUser"), "msg-2")
        );
    }

    private static Stream<Arguments> userEventProvider() {
        return Stream.of(
                Arguments.of(
                        new UserBalanceDebitFailedEvent(UUID.randomUUID(), "testUser"), "msg-3")
        );
    }

    private static Stream<Arguments> orderEventProvider() {
        return Stream.of(
                Arguments.of(
                        new OrderCompletionFailedEvent(UUID.randomUUID(), UUID.randomUUID(),
                                "testUser", BigDecimal.TEN), "msg-4")
        );
    }
}
