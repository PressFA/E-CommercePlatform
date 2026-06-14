package by.pressf.productms.unit.kafka.handler;

import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.productms.dto.internal.ProductReservationRequest;
import by.pressf.productms.exception.ProductHistoryNotFoundException;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundByOrderIdException;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.kafka.handler.ProductCommandsHandler;
import by.pressf.productms.kafka.publisher.KafkaEventPublisher;
import by.pressf.productms.service.IdempotencyService;
import by.pressf.productms.service.ProductHistoryService;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
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
public class ProductCommandsHandlerUnitTests {
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @Mock IdempotencyService idempotencyService;
    private @Mock ProductHistoryService productHistoryService;
    private @InjectMocks ProductCommandsHandler handler;

    @ParameterizedTest @MethodSource("reserveArgs")
    void handleReserveProduct_Success_CompletesSuccessfully(ReserveProductCommand command,
                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        when(productHistoryService.reserveProduct(any(ProductReservationRequest.class))).thenReturn(BigDecimal.TEN);
        doNothing().when(kafkaEventPublisher).sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleReserveProductCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .reserveProduct(any(ProductReservationRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("reserveArgs")
    void handleReserveProduct_Duplicate_ThrowsDuplicateMessageException(ReserveProductCommand command,
                                                                        String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleReserveProductCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, never())
                .reserveProduct(any(ProductReservationRequest.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("reserveArgs")
    void handleReserveProduct_OptimisticLockingFailure_ThrowsOptimisticLockingFailureException(ReserveProductCommand command,
                                                                                               String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(OptimisticLockingFailureException.class)).when(productHistoryService)
                .reserveProduct(any(ProductReservationRequest.class));

        // Act & Assert
        assertThrows(OptimisticLockingFailureException.class,
                () -> handler.handleReserveProductCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .reserveProduct(any(ProductReservationRequest.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("reserveArgs")
    void handleReserveProduct_ProductNotFound_ThrowsProductNotFoundException(ReserveProductCommand command,
                                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(ProductNotFoundException.class)).when(productHistoryService)
                .reserveProduct(any(ProductReservationRequest.class));

        // Act & Assert
        assertThrows(ProductNotFoundException.class,
                () -> handler.handleReserveProductCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .reserveProduct(any(ProductReservationRequest.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("reserveArgs")
    void handleReserveProduct_ProductInsufficient_ThrowsProductInsufficientException(ReserveProductCommand command,
                                                                                     String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(ProductInsufficientException.class)).when(productHistoryService)
                .reserveProduct(any(ProductReservationRequest.class));

        // Act & Assert
        assertThrows(ProductInsufficientException.class,
                () -> handler.handleReserveProductCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .reserveProduct(any(ProductReservationRequest.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("reserveArgs")
    void handleReserveProduct_KafkaError_ThrowsKafkaException(ReserveProductCommand command,
                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        when(productHistoryService.reserveProduct(any(ProductReservationRequest.class))).thenReturn(BigDecimal.TEN);
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleReserveProductCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .reserveProduct(any(ProductReservationRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("reserveArgs")
    void handleReserveProduct_DataAccessError_ThrowsDataAccessException(ReserveProductCommand command,
                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        when(productHistoryService.reserveProduct(any(ProductReservationRequest.class))).thenReturn(BigDecimal.TEN);
        doNothing().when(kafkaEventPublisher).sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleReserveProductCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .reserveProduct(any(ProductReservationRequest.class));
        verify(kafkaEventPublisher, times(1))
                .sendProductReservedEvent(anyString(), any(ProductReservedEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> reserveArgs() {
        return Stream.of(
                Arguments.of(new ReserveProductCommand(UUID.randomUUID(), UUID.randomUUID(),
                        UUID.randomUUID(), "testUser", 1), "msg-123")
        );
    }

    @ParameterizedTest @MethodSource("cancelArgs")
    void handleCancelProduct_Success_CompletesSuccessfully(CancelProductReservationCommand command,
                                                           String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(productHistoryService)
                .cancelProductReservation(any(UUID.class));
        doNothing().when(kafkaEventPublisher)
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleCancelProductReservationCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .cancelProductReservation(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("cancelArgs")
    void handleCancelProduct_Duplicate_ThrowsDuplicateMessageException(CancelProductReservationCommand command,
                                                                       String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleCancelProductReservationCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, never())
                .cancelProductReservation(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("cancelArgs")
    void handleCancelProduct_OptimisticLockingFailure_ThrowsOptimisticLockingFailureException(CancelProductReservationCommand command,
                                                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doThrow(mock(OptimisticLockingFailureException.class)).when(productHistoryService)
                .cancelProductReservation(any(UUID.class));

        // Act & Assert
        assertThrows(OptimisticLockingFailureException.class,
                () -> handler.handleCancelProductReservationCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .cancelProductReservation(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("cancelArgs")
    void handleCancelProduct_ProductHistoryNotFound_ThrowsProductHistoryNotFoundException(CancelProductReservationCommand command,
                                                                                          String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doThrow(mock(ProductHistoryNotFoundException.class)).when(productHistoryService)
                .cancelProductReservation(any(UUID.class));

        // Act & Assert
        assertThrows(ProductHistoryNotFoundException.class,
                () -> handler.handleCancelProductReservationCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .cancelProductReservation(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("cancelArgs")
    void handleCancelProduct_ProductNotFoundByOrderId_ThrowsProductNotFoundByOrderIdException(CancelProductReservationCommand command,
                                                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doThrow(mock(ProductNotFoundByOrderIdException.class)).when(productHistoryService)
                .cancelProductReservation(any(UUID.class));

        // Act & Assert
        assertThrows(ProductNotFoundByOrderIdException.class,
                () -> handler.handleCancelProductReservationCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .cancelProductReservation(any(UUID.class));
        verify(kafkaEventPublisher, never())
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("cancelArgs")
    void handleCancelProduct_KafkaError_ThrowsKafkaException(CancelProductReservationCommand command,
                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(productHistoryService)
                .cancelProductReservation(any(UUID.class));
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleCancelProductReservationCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .cancelProductReservation(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("cancelArgs")
    void handleCancelProduct_DataAccessError_ThrowsDataAccessException(CancelProductReservationCommand command,
                                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(productHistoryService)
                .cancelProductReservation(any(UUID.class));
        doNothing().when(kafkaEventPublisher)
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleCancelProductReservationCommand(command, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(productHistoryService, times(1))
                .cancelProductReservation(any(UUID.class));
        verify(kafkaEventPublisher, times(1))
                .sendProductReservationCanceledEvent(anyString(), any(ProductReservationCanceledEvent.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> cancelArgs() {
        return Stream.of(
                Arguments.of(new CancelProductReservationCommand(UUID.randomUUID(), "testUser"),
                        "msg-456")
        );
    }
}
