package by.pressf.orderms.unit.kafka.handler;

import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.orderms.exception.OrderNotFoundException;
import by.pressf.orderms.kafka.handler.OrderCommandsHandler;
import by.pressf.orderms.kafka.publisher.KafkaEventPublisher;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderService;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OrderCommandsHandlerUnitTests {
    private @Mock OrderService orderService;
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @Mock IdempotencyService idempotencyService;
    private @InjectMocks OrderCommandsHandler handler;

    @ParameterizedTest @MethodSource("confirmArgs")
    void handleConfirmOrder_Success_CompletesSuccessfully(ConfirmOrderCommand command,
                                                          String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(orderService).approveOrder(any(UUID.class));
        doNothing().when(kafkaEventPublisher)
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleConfirmOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .approveOrder(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("confirmArgs")
    void handleConfirmOrder_Duplicate_ThrowsDuplicateMessageException(ConfirmOrderCommand command,
                                                                      String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleConfirmOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, never())
                .approveOrder(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("confirmArgs")
    void handleConfirmOrder_OrderNotFound_ThrowsOrderNotFoundException(ConfirmOrderCommand command,
                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(OrderNotFoundException.class)).when(orderService)
                .approveOrder(any(UUID.class));

        // Act & Assert
        assertThrows(OrderNotFoundException.class,
                () -> handler.handleConfirmOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .approveOrder(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("confirmArgs")
    void handleConfirmOrder_KafkaError_ThrowsKafkaException(ConfirmOrderCommand command,
                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(orderService).approveOrder(any(UUID.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleConfirmOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .approveOrder(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("confirmArgs")
    void handleConfirmOrder_DataAccessError_ThrowsDataAccessException(ConfirmOrderCommand command,
                                                                      String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(orderService).approveOrder(any(UUID.class));
        doNothing().when(kafkaEventPublisher)
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleConfirmOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .approveOrder(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendOrderCompletedEvent(anyString(), any(OrderCompletedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> confirmArgs() {
        return Stream.of(
                Arguments.of(new ConfirmOrderCommand(UUID.randomUUID(), UUID.randomUUID(),
                        "orderUser", BigDecimal.TEN), "msg-confirm-111")
        );
    }


    @ParameterizedTest @MethodSource("rejectArgs")
    void handleRejectOrder_Success_CompletesSuccessfully(RejectOrderCommand command,
                                                         String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(orderService).rejectOrder(any(UUID.class));
        doNothing().when(kafkaEventPublisher)
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleRejectOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .rejectOrder(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("rejectArgs")
    void handleRejectOrder_Duplicate_ThrowsDuplicateMessageException(RejectOrderCommand command,
                                                                     String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleRejectOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, never())
                .rejectOrder(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("rejectArgs")
    void handleRejectOrder_OrderNotFound_ThrowsOrderNotFoundException(RejectOrderCommand command,
                                                                      String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(OrderNotFoundException.class)).when(orderService)
                .rejectOrder(any(UUID.class));

        // Act & Assert
        assertThrows(OrderNotFoundException.class,
                () -> handler.handleRejectOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .rejectOrder(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("rejectArgs")
    void handleRejectOrder_KafkaError_ThrowsKafkaException(RejectOrderCommand command,
                                                           String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(orderService).rejectOrder(any(UUID.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleRejectOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .rejectOrder(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("rejectArgs")
    void handleRejectOrder_DataAccessError_ThrowsDataAccessException(RejectOrderCommand command,
                                                                     String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(orderService).rejectOrder(any(UUID.class));
        doNothing().when(kafkaEventPublisher)
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleRejectOrderCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .rejectOrder(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendOrderRejectedEvent(anyString(), any(OrderRejectedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> rejectArgs() {
        return Stream.of(
                Arguments.of(new RejectOrderCommand(UUID.randomUUID(), "orderUser"),
                        "msg-reject-222")
        );
    }
}
