package by.pressf.orderms.unit.kafka.handler;

import by.pressf.core.dto.orchestration.events.cart.CreateOrderShoppingCart;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.orderms.dto.internal.OrderCreationData;
import by.pressf.orderms.kafka.handler.ROrderWCartEventsHandler;
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

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ROrderWCartEventsHandlerUnitTests {
    private @Mock OrderService orderService;
    private @Mock IdempotencyService idempotencyService;
    private @InjectMocks ROrderWCartEventsHandler handler;

    @ParameterizedTest @MethodSource("arguments")
    void handleCreateOrderShoppingCart_Success_CompletesSuccessfully(CreateOrderShoppingCart event,
                                                                     String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        when(orderService.createOrder(any(OrderCreationData.class))).thenReturn(UUID.randomUUID());
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleCreateOrderShoppingCart(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .createOrder(any(OrderCreationData.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleCreateOrderShoppingCart_Duplicate_ThrowsDuplicateMessageException(CreateOrderShoppingCart event,
                                                                                 String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleCreateOrderShoppingCart(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, never())
                .createOrder(any(OrderCreationData.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleCreateOrderShoppingCart_KafkaError_ThrowsKafkaException(CreateOrderShoppingCart event,
                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(KafkaException.class)).when(orderService)
                .createOrder(any(OrderCreationData.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleCreateOrderShoppingCart(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .createOrder(any(OrderCreationData.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleCreateOrderShoppingCart_DataAccessError_ThrowsDataAccessException(CreateOrderShoppingCart event,
                                                                                 String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        when(orderService.createOrder(any(OrderCreationData.class))).thenReturn(UUID.randomUUID());
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleCreateOrderShoppingCart(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderService, times(1))
                .createOrder(any(OrderCreationData.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(new CreateOrderShoppingCart(UUID.randomUUID(), "cartUser",
                        UUID.randomUUID(), 2), "msg-cart-111")
        );
    }
}
