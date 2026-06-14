package by.pressf.orderms.unit.service;

import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.OrderRepository;
import by.pressf.orderms.dto.internal.OrderCreationData;
import by.pressf.orderms.exception.OrderNotFoundException;
import by.pressf.orderms.kafka.publisher.KafkaEventPublisher;
import by.pressf.orderms.service.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.KafkaException;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OrderServiceUnitTests {
    private @Mock OrderRepository orderRepository;
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @InjectMocks OrderService orderService;

    @ParameterizedTest @MethodSource("orderCreationDataProvider")
    void createOrder_RepositorySaveFails_PropagatesDataAccessException(OrderCreationData creationData) {
        // Arrange
        when(orderRepository.save(any(OrderEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> orderService.createOrder(creationData));
        verify(orderRepository, times(1)).save(any(OrderEntity.class));
        verify(kafkaEventPublisher, never()).sendOrderCreatedEvent(anyString(), any(OrderCreatedEvent.class));
    }

    @ParameterizedTest @MethodSource("orderCreationDataProvider")
    void createOrder_KafkaPublisherFails_PropagatesKafkaException(OrderCreationData creationData) {
        // Arrange
        UUID expectedId = UUID.randomUUID();
        doAnswer(invocation -> {
            OrderEntity entity = invocation.getArgument(0);
            entity.setId(expectedId);
            return entity;
        }).when(orderRepository).save(any(OrderEntity.class));

        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendOrderCreatedEvent(anyString(), any(OrderCreatedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class, () -> orderService.createOrder(creationData));
        verify(orderRepository, times(1)).save(any(OrderEntity.class));
    }

    @ParameterizedTest @MethodSource("orderCreationDataProvider")
    void createOrder_ValidData_ReturnsGeneratedId(OrderCreationData creationData) {
        // Arrange
        UUID expectedId = UUID.randomUUID();
        doAnswer(invocation -> {
            OrderEntity entity = invocation.getArgument(0);
            entity.setId(expectedId);
            return entity;
        }).when(orderRepository).save(any(OrderEntity.class));

        // Act
        UUID actualId = orderService.createOrder(creationData);

        // Assert
        assertThat(actualId).isEqualTo(expectedId);
        verify(orderRepository, times(1)).save(any(OrderEntity.class));
        verify(kafkaEventPublisher, times(1))
                .sendOrderCreatedEvent(eq(expectedId.toString()), any(OrderCreatedEvent.class));
    }

    private static Stream<Arguments> orderCreationDataProvider() {
        return Stream.of(
                Arguments.of(new OrderCreationData(UUID.randomUUID(), "john_doe",
                        UUID.randomUUID(), 5))
        );
    }

    @Test
    void approveOrder_OrderNotFound_ThrowsOrderNotFoundException() {
        // Arrange
        UUID orderId = UUID.randomUUID();
        when(orderRepository.findById(any(UUID.class))).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(OrderNotFoundException.class, () -> orderService.approveOrder(orderId));
        verify(orderRepository, never()).save(any(OrderEntity.class));
    }

    @ParameterizedTest @MethodSource("orderProvider")
    void approveOrder_RepositorySaveFails_PropagatesDataAccessException(UUID orderId, OrderEntity order) {
        // Arrange
        when(orderRepository.findById(any(UUID.class))).thenReturn(Optional.of(order));
        when(orderRepository.save(any(OrderEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> orderService.approveOrder(orderId));
    }

    @ParameterizedTest @MethodSource("orderProvider")
    void approveOrder_ValidId_UpdatesStatusToApproved(UUID orderId, OrderEntity order) {
        // Arrange
        when(orderRepository.findById(any(UUID.class))).thenReturn(Optional.of(order));
        when(orderRepository.save(any(OrderEntity.class))).thenReturn(order);

        // Act
        orderService.approveOrder(orderId);

        // Assert
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        verify(orderRepository, times(1)).save(order);
    }

    @Test
    void rejectOrder_OrderNotFound_ThrowsOrderNotFoundException() {
        // Arrange
        UUID orderId = UUID.randomUUID();
        when(orderRepository.findById(any(UUID.class))).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(OrderNotFoundException.class, () -> orderService.rejectOrder(orderId));
        verify(orderRepository, never()).save(any(OrderEntity.class));
    }

    @ParameterizedTest @MethodSource("orderProvider")
    void rejectOrder_RepositorySaveFails_PropagatesDataAccessException(UUID orderId, OrderEntity order) {
        // Arrange
        when(orderRepository.findById(any(UUID.class))).thenReturn(Optional.of(order));
        when(orderRepository.save(any(OrderEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> orderService.rejectOrder(orderId));
    }

    @ParameterizedTest @MethodSource("orderProvider")
    void rejectOrder_ValidId_UpdatesStatusToRejected(UUID orderId, OrderEntity order) {
        // Arrange
        when(orderRepository.findById(any(UUID.class))).thenReturn(Optional.of(order));
        when(orderRepository.save(any(OrderEntity.class))).thenReturn(order);

        // Act
        orderService.rejectOrder(orderId);

        // Assert
        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        verify(orderRepository, times(1)).save(order);
    }

    private static Stream<Arguments> orderProvider() {
        UUID orderId = UUID.randomUUID();
        return Stream.of(
                Arguments.of(
                        orderId,
                        OrderEntity.builder()
                                .id(orderId)
                                .userId(UUID.randomUUID())
                                .productId(UUID.randomUUID())
                                .quantity(10)
                                .status(OrderStatus.CREATED)
                                .build()
                )
        );
    }
}
