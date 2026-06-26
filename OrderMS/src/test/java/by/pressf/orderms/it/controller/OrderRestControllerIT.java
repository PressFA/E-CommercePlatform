package by.pressf.orderms.it.controller;

import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dto.incoming.CreateOrderRequest;
import by.pressf.orderms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class OrderRestControllerIT extends BaseIT {
    @BeforeAll
    static void init() { spyConsumer = createSpyConsumer(List.of("successful-events")); }

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        orderRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test
    void should_CreateOrderAndPublishEvent_When_RequestIsValid() throws Exception {
        // Arrange
        CreateOrderRequest request = new CreateOrderRequest(
                UUID.randomUUID(),
                "danny.black@example.com",
                UUID.randomUUID(),
                3
        );
        String jsonRequest = mapper.writeValueAsString(request);
        String successfulEventsTopic = env.getRequiredProperty("successful-events.topic.name");

        // Act
        MvcResult result = mockMvc.perform(post("/api/v1/order")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonRequest))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").exists())
                .andReturn();

        ConsumerRecord<String, String> kafkaRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, successfulEventsTopic, Duration.ofSeconds(5));

        // Assert
        String responseBody = result.getResponse().getContentAsString();
        Map<?, ?> responseMap = mapper.readValue(responseBody, Map.class);
        UUID orderId = UUID.fromString(responseMap.get("orderId").toString());

        List<OrderEntity> orders = orderRepository.findAll();
        assertThat(orders).hasSize(1);
        OrderEntity savedOrder = orders.getFirst();
        assertThat(savedOrder.getId()).isEqualTo(orderId);
        assertThat(savedOrder.getUserId()).isEqualTo(request.userId());
        assertThat(savedOrder.getProductId()).isEqualTo(request.productId());
        assertThat(savedOrder.getQuantity()).isEqualTo(request.quantity());
        assertThat(savedOrder.getStatus()).isEqualTo(OrderStatus.CREATED);

        assertThat(kafkaRecord.key()).isEqualTo(orderId.toString());
        assertThat(kafkaRecord.value()).isNotNull();
        assertThat(kafkaRecord.headers().lastHeader("messageId")).isNotNull();
    }

    @Test
    void should_ReturnBadRequestAndValidationErrors_When_FieldsAreInvalid() throws Exception {
        // Arrange
        CreateOrderRequest invalidRequest = new CreateOrderRequest(
                null,
                "not-an-email",
                UUID.randomUUID(),
                15
        );
        String jsonRequest = mapper.writeValueAsString(invalidRequest);
        String successfulEventsTopic = env.getRequiredProperty("successful-events.topic.name");

        // Act
        mockMvc.perform(post("/api/v1/order")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonRequest))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.userId").value("The userId field is empty!"))
                .andExpect(jsonPath("$.username").value("Invalid email address!"))
                .andExpect(jsonPath("$.quantity").value("The maximum order quantity is 10 pieces!"));

        // Assert
        assertThat(orderRepository.findAll()).isEmpty();
        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, successfulEventsTopic, Duration.ofSeconds(2)));
    }
}
