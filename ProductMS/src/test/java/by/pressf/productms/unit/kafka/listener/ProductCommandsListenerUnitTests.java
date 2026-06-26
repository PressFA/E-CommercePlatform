package by.pressf.productms.unit.kafka.listener;

import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.productms.exception.ProductHistoryNotFoundException;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundByOrderIdException;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.kafka.handler.ProductCommandsHandler;
import by.pressf.productms.kafka.listener.ProductCommandsListener;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ProductCommandsListenerUnitTests {
    private @Mock Environment env;
    private @Mock ProductCommandsHandler handler;
    private @InjectMocks ProductCommandsListener listener;

    private static final String SUCCESS_ERROR_TOPIC = "errors-successful-events-topic";
    private static final String COMPENSATE_ERROR_TOPIC = "errors-compensating-events-topic";

    @ParameterizedTest @MethodSource("handleReserveCommand")
    void handleReserveCommand_Success_ReturnsNormally(ReserveProductCommand command,
                                                      String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleReserveCommand")
    void handleReserveCommand_DuplicateMessage_SwallowsException(ReserveProductCommand command,
                                                                 String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleReserveCommand")
    void handleReserveCommand_OptimisticLockingFailure_ThrowsRetryableException(ReserveProductCommand command,
                                                                                String messageId) {
        // Arrange
        doThrow(mock(OptimisticLockingFailureException.class))
                .when(handler).handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        assertInstanceOf(OptimisticLockingFailureException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleReserveCommand")
    void handleReserveCommand_ProductNotFound_ThrowsNotRetryableException(ReserveProductCommand command,
                                                                          String messageId) {
        // Arrange
        doThrow(mock(ProductNotFoundException.class)).when(handler)
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        assertInstanceOf(ProductNotFoundException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleReserveCommand")
    void handleReserveCommand_ProductInsufficient_ThrowsNotRetryableException(ReserveProductCommand command,
                                                                              String messageId) {
        // Arrange
        doThrow(mock(ProductInsufficientException.class)).when(handler)
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        assertInstanceOf(ProductInsufficientException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleReserveCommand")
    void handleReserveCommand_DataAccessException_ThrowsNotRetryableException(ReserveProductCommand command,
                                                                              String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name"))
                .thenReturn(SUCCESS_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleReserveProductCommand(any(ReserveProductCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
        assertEquals(SUCCESS_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationFailedEvent.class, exception.getValue());
    }

    private static Stream<Arguments> handleReserveCommand() {
        return Stream.of(Arguments.of(
                new ReserveProductCommand(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                        "testUser", 1), "msg-res-123")
        );
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_Success_ReturnsNormally(CancelProductReservationCommand command,
                                                     String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_DuplicateMessage_SwallowsException(CancelProductReservationCommand command,
                                                                String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_OptimisticLockingFailure_ThrowsRetryableException(CancelProductReservationCommand command,
                                                                               String messageId) {
        // Arrange
        doThrow(mock(OptimisticLockingFailureException.class)).when(handler)
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        RetryableException exception = assertThrows(RetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        assertInstanceOf(OptimisticLockingFailureException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationCancelFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_ProductHistoryNotFound_ThrowsNotRetryableException(CancelProductReservationCommand command,
                                                                                String messageId) {
        // Arrange
        doThrow(mock(ProductHistoryNotFoundException.class)).when(handler)
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        assertInstanceOf(ProductHistoryNotFoundException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationCancelFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_ProductNotFoundByOrderId_ThrowsNotRetryableException(CancelProductReservationCommand command,
                                                                                  String messageId) {
        // Arrange
        doThrow(mock(ProductNotFoundByOrderIdException.class)).when(handler)
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        assertInstanceOf(ProductNotFoundByOrderIdException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationCancelFailedEvent.class, exception.getValue());
    }

    @ParameterizedTest @MethodSource("handleCancelCommand")
    void handleCancelCommand_DataAccessException_ThrowsNotRetryableException(CancelProductReservationCommand command,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATE_ERROR_TOPIC);

        // Act & Assert
        NotRetryableException exception = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleCancelProductReservationCommand(any(CancelProductReservationCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, exception.getCause());
        assertEquals(COMPENSATE_ERROR_TOPIC, exception.getTopicName());
        assertEquals(command.orderId().toString(), exception.getKey());
        assertInstanceOf(ProductReservationCancelFailedEvent.class, exception.getValue());
    }

    private static Stream<Arguments> handleCancelCommand() {
        return Stream.of(Arguments.of(
                new CancelProductReservationCommand(UUID.randomUUID(), "testUser"),
                "msg-can-123"
        ));
    }
}
