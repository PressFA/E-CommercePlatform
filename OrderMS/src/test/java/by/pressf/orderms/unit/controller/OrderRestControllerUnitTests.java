package by.pressf.orderms.unit.controller;

import by.pressf.orderms.controller.OrderRestController;
import by.pressf.orderms.dto.incoming.CreateOrderRequest;
import by.pressf.orderms.service.OrderService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.kafka.KafkaException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderRestController.class)
public class OrderRestControllerUnitTests {
    @MockitoBean
    private OrderService orderService;
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper mapper;

    @ParameterizedTest @MethodSource("provideInvalidOrders")
    void createOrder_InvalidData_ReturnsBadRequest(CreateOrderRequest invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(post("/api/v1/order")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(orderService);
    }

    private static Stream<Arguments> provideInvalidOrders() {
        UUID validId = UUID.randomUUID();
        return Stream.of(
                Arguments.of(new CreateOrderRequest(null, "test@test.com", validId, 1)),
                Arguments.of(new CreateOrderRequest(validId, "", validId, 1)),
                Arguments.of(new CreateOrderRequest(validId, "invalid", validId, 1)),
                Arguments.of(new CreateOrderRequest(validId, "test@test.com", null, 1)),
                Arguments.of(new CreateOrderRequest(validId, "test@test.com", validId, null)),
                Arguments.of(new CreateOrderRequest(validId, "test@test.com", validId, 0)),
                Arguments.of(new CreateOrderRequest(validId, "test@test.com", validId, 11))
        );
    }

    @Test
    void createOrder_DatabaseError_ReturnsInternalServerError() throws Exception {
        // Arrange
        CreateOrderRequest request =
                new CreateOrderRequest(UUID.randomUUID(), "test@test.com", UUID.randomUUID(), 5);

        when(orderService.createOrder(any())).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/order")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(orderService, times(1)).createOrder(any());
    }

    @Test
    void createOrder_KafkaError_ReturnsInternalServerError() throws Exception {
        // Arrange
        CreateOrderRequest request =
                new CreateOrderRequest(UUID.randomUUID(), "test@test.com", UUID.randomUUID(), 5);

        when(orderService.createOrder(any())).thenThrow(mock(KafkaException.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/order")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("An unexpected error occurred")
                );

        verify(orderService, times(1)).createOrder(any());
    }

    @Test
    void createOrder_ValidData_ReturnsCreated() throws Exception {
        // Arrange
        UUID orderId = UUID.randomUUID();
        CreateOrderRequest request =
                new CreateOrderRequest(UUID.randomUUID(), "test@test.com", UUID.randomUUID(), 5);

        when(orderService.createOrder(any())).thenReturn(orderId);

        // Act & Assert
        mockMvc.perform(post("/api/v1/order")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isCreated(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.orderId").exists()
                );

        verify(orderService, times(1)).createOrder(any());
    }
}
