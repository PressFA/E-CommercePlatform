package by.pressf.orderms.unit.kafka.listener;

import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.exception.OrderNotFoundException;
import by.pressf.orderms.kafka.handler.OrderCommandsHandler;
import by.pressf.orderms.kafka.listener.OrderCommandsListener;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OrderCommandsListenerUnitTests {
    private @Mock Environment env;
    private @Mock OrderCommandsHandler handler;
    private @InjectMocks OrderCommandsListener listener;

    private static final String SUCCESS_TOPIC = "errors-successful-events-topic";
    private static final String COMPENSATING_TOPIC = "errors-compensating-events-topic";

    @ParameterizedTest @MethodSource("confirmCommandProvider")
    void handleConfirmOrder_Success_ReturnsNormally(ConfirmOrderCommand command,
                                                    String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("confirmCommandProvider")
    void handleConfirmOrder_Duplicate_SwallowsException(ConfirmOrderCommand command,
                                                        String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("confirmCommandProvider")
    void handleConfirmOrder_OrderNotFound_ThrowsNotRetryable(ConfirmOrderCommand command,
                                                             String messageId) {
        // Arrange
        doThrow(mock(OrderNotFoundException.class)).when(handler)
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name")).thenReturn(SUCCESS_TOPIC);

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());

        assertInstanceOf(OrderNotFoundException.class, ex.getCause());
        assertEquals(SUCCESS_TOPIC, ex.getTopicName());
        assertEquals(command.orderId().toString(), ex.getKey());
        assertInstanceOf(OrderCompletionFailedEvent.class, ex.getValue());
    }

    @ParameterizedTest @MethodSource("confirmCommandProvider")
    void handleConfirmOrder_DataAccessError_ThrowsNotRetryable(ConfirmOrderCommand command,
                                                               String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());

        when(env.getRequiredProperty("errors-successful-events.topic.name")).thenReturn(SUCCESS_TOPIC);

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleConfirmOrderCommand(any(ConfirmOrderCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
        assertEquals(SUCCESS_TOPIC, ex.getTopicName());
        assertEquals(command.orderId().toString(), ex.getKey());
        assertInstanceOf(OrderCompletionFailedEvent.class, ex.getValue());
    }

    private static Stream<Arguments> confirmCommandProvider() {
        return Stream.of(
                Arguments.of(new ConfirmOrderCommand(UUID.randomUUID(), UUID.randomUUID(),
                        "user", BigDecimal.TEN), "msg-1")
        );
    }

    @ParameterizedTest @MethodSource("rejectCommandProvider")
    void handleRejectOrder_Success_ReturnsNormally(RejectOrderCommand command,
                                                   String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("rejectCommandProvider")
    void handleRejectOrder_Duplicate_SwallowsException(RejectOrderCommand command,
                                                       String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());
    }

    @ParameterizedTest @MethodSource("rejectCommandProvider")
    void handleRejectOrder_OrderNotFound_ThrowsNotRetryable(RejectOrderCommand command,
                                                            String messageId) {
        // Arrange
        doThrow(mock(OrderNotFoundException.class)).when(handler)
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATING_TOPIC);

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());

        assertInstanceOf(OrderNotFoundException.class, ex.getCause());
        assertEquals(COMPENSATING_TOPIC, ex.getTopicName());
        assertEquals(command.orderId().toString(), ex.getKey());
        assertInstanceOf(OrderRejectionFailedEvent.class, ex.getValue());
    }

    @ParameterizedTest @MethodSource("rejectCommandProvider")
    void handleRejectOrder_DataAccessError_ThrowsNotRetryable(RejectOrderCommand command,
                                                              String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());

        when(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .thenReturn(COMPENSATING_TOPIC);

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handleCommand(command, messageId));

        verify(handler, times(1))
                .handleRejectOrderCommand(any(RejectOrderCommand.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
        assertEquals(COMPENSATING_TOPIC, ex.getTopicName());
        assertEquals(command.orderId().toString(), ex.getKey());
        assertInstanceOf(OrderRejectionFailedEvent.class, ex.getValue());
    }

    private static Stream<Arguments> rejectCommandProvider() {
        return Stream.of(
                Arguments.of(new RejectOrderCommand(UUID.randomUUID(), "user"), "msg-2")
        );
    }
}
