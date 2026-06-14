package by.pressf.orderms.unit.kafka.listener;

import by.pressf.core.dto.orchestration.events.cart.CreateOrderShoppingCart;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.kafka.handler.ROrderWCartEventsHandler;
import by.pressf.orderms.kafka.listener.ROrderWCartEventsListener;
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
public class ROrderWCartEventsListenerUnitTests {
    private @Mock ROrderWCartEventsHandler handler;
    private @InjectMocks ROrderWCartEventsListener listener;

    @ParameterizedTest @MethodSource("eventProvider")
    void handleCreateOrder_Success_ReturnsNormally(CreateOrderShoppingCart event,
                                                   String messageId) {
        // Arrange
        doNothing().when(handler)
                .handleCreateOrderShoppingCart(any(CreateOrderShoppingCart.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleCreateOrderShoppingCart(any(CreateOrderShoppingCart.class), anyString());
    }

    @ParameterizedTest @MethodSource("eventProvider")
    void handleCreateOrder_Duplicate_SwallowsException(CreateOrderShoppingCart event,
                                                       String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(handler)
                .handleCreateOrderShoppingCart(any(CreateOrderShoppingCart.class), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleCreateOrderShoppingCart(any(CreateOrderShoppingCart.class), anyString());
    }

    @ParameterizedTest @MethodSource("eventProvider")
    void handleCreateOrder_DataAccessError_ThrowsNotRetryable(CreateOrderShoppingCart event,
                                                              String messageId) {
        // Arrange
        doThrow(mock(DataAccessException.class)).when(handler)
                .handleCreateOrderShoppingCart(any(CreateOrderShoppingCart.class), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(handler, times(1))
                .handleCreateOrderShoppingCart(any(CreateOrderShoppingCart.class), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    private static Stream<Arguments> eventProvider() {
        return Stream.of(
                Arguments.of(
                        new CreateOrderShoppingCart(UUID.randomUUID(), "testUser",
                                UUID.randomUUID(), 1), "msg-1")
        );
    }
}
