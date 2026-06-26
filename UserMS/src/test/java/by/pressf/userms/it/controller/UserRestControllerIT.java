package by.pressf.userms.it.controller;

import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dto.incoming.CreateUserRequest;
import by.pressf.userms.dto.incoming.TopUpBalanceRequest;
import by.pressf.userms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

public class UserRestControllerIT extends BaseIT {
    @BeforeAll
    static void init() { spyConsumer = createSpyConsumer(List.of("r-payment-w-user-events")); }

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        userRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test
    void should_ReturnInternalServerError_When_UsernameAlreadyExists() throws Exception {
        // Arrange
        userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("100.00"))
                .build());

        CreateUserRequest request = new CreateUserRequest(
                "test@mail.com",
                "12345678",
                "Danny_Black"
        );

        // Act & Assert
        mockMvc.perform(post("/api/v1/user")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );
    }

    @Test
    void should_CreateUserAndReturnCreatedStatus_When_RequestIsValid() throws Exception {
        // Arrange
        CreateUserRequest request = new CreateUserRequest(
                "test@mail.com",
                "12345678",
                "Danny_Black"
        );

        // Act & Assert
        mockMvc.perform(post("/api/v1/user")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isCreated(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.userId").exists()
                );

        assertThat(userRepository.findAll()).hasSize(1);
    }

    @Test
    void should_ReturnNotFoundAndNotPublishEvent_When_UserDoesNotExistDuringTopUp() throws Exception {
        // Arrange
        TopUpBalanceRequest request = new TopUpBalanceRequest(
                UUID.randomUUID(),
                BigDecimal.TEN
        );

        String topic = env.getRequiredProperty("r-payment-w-user.topic.name");

        // Act & Assert
        mockMvc.perform(patch("/api/v1/user/balance/top-up")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isNotFound(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error")
                                .value("User with id " + request.userId() + " not found")
                );

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(3)));
    }

    @Test
    void should_TopUpBalanceAndPublishEvent_When_UserExistsAndRequestIsValid() throws Exception {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("90.00"))
                .build());

        TopUpBalanceRequest request = new TopUpBalanceRequest(
                user.getId(),
                BigDecimal.TEN
        );

        String topic = env.getRequiredProperty("r-payment-w-user.topic.name");

        // Act & Assert
        mockMvc.perform(patch("/api/v1/user/balance/top-up")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isOk(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.name").value("Danny"),
                        jsonPath("$.balance").value(100)
                );

        user = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user.getBalance()).isEqualByComparingTo(new BigDecimal("100.00"));

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        assertThat(record.key()).isEqualTo(user.getId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceCreditedEvent event = mapper.readValue(record.value(), UserBalanceCreditedEvent.class);
        assertThat(event.userId()).isEqualTo(user.getId());
        assertThat(event.email()).isEqualTo(user.getUsername());
        assertThat(event.amount()).isEqualByComparingTo(request.amount());
    }
}
