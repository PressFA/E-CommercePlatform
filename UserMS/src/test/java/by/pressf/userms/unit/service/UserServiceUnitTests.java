package by.pressf.userms.unit.service;

import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dao.repository.UserRepository;
import by.pressf.userms.dto.incoming.CreateUserRequest;
import by.pressf.userms.dto.incoming.TopUpBalanceRequest;
import by.pressf.userms.dto.internal.UserBalanceRequest;
import by.pressf.userms.dto.internal.UserBalanceResponse;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.publisher.KafkaEventPublisher;
import by.pressf.userms.service.UserService;
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

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class UserServiceUnitTests {
    private @Mock UserRepository userRepository;
    private @Mock KafkaEventPublisher kafkaEventPublisher;
    private @InjectMocks UserService userService;

    @Test
    void createUser_RepositorySaveFails_PropagatesDataAccessException() {
        // Arrange
        CreateUserRequest creationData =
                new CreateUserRequest("john_doe", "password123", "John");
        when(userRepository.save(any(UserEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> userService.createUser(creationData));
        verify(userRepository, times(1)).save(any(UserEntity.class));
    }

    @ParameterizedTest @MethodSource("createUserProvider")
    void createUser_ValidData_ReturnsGeneratedId(CreateUserRequest userRequest) {
        // Arrange
        UUID expectedId = UUID.randomUUID();
        doAnswer(invocation -> {
            UserEntity entity = invocation.getArgument(0);
            entity.setId(expectedId);
            return entity;
        }).when(userRepository).save(any(UserEntity.class));

        // Act
        UUID actualId = userService.createUser(userRequest);

        // Assert
        assertThat(actualId).isEqualTo(expectedId);
        verify(userRepository, times(1)).save(any(UserEntity.class));
    }

    private static Stream<Arguments> createUserProvider() {
        return Stream.of(
                Arguments.of(new CreateUserRequest("john_doe", "password123", "John"))
        );
    }

    @Test
    void topUpUserBalance_UserNotFound_ThrowsUserNotFoundException() {
        // Arrange
        TopUpBalanceRequest req = new TopUpBalanceRequest(UUID.randomUUID(), new BigDecimal("50.00"));
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(UserNotFoundException.class, () -> userService.topUpUserBalance(req));
        verify(userRepository, never()).save(any(UserEntity.class));
    }

    @ParameterizedTest @MethodSource("topUpUserBalance")
    void topUpUserBalance_RepositorySaveFails_PropagatesDataAccessException(TopUpBalanceRequest req,
                                                                            UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> userService.topUpUserBalance(req));
        verify(kafkaEventPublisher, never())
                .sendUserBalanceCreditedEvent(anyString(), any(UserBalanceCreditedEvent.class));
    }

    @ParameterizedTest @MethodSource("topUpUserBalance")
    void topUpUserBalance_KafkaPublisherFails_PropagatesKafkaException(TopUpBalanceRequest req,
                                                                       UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenReturn(user);
        doThrow(mock(KafkaException.class)).when(kafkaEventPublisher)
                .sendUserBalanceCreditedEvent(anyString(), any(UserBalanceCreditedEvent.class));

        // Act & Assert
        assertThrows(KafkaException.class, () -> userService.topUpUserBalance(req));
    }

    @ParameterizedTest @MethodSource("topUpUserBalance")
    void topUpUserBalance_ValidRequest_SendsEventAndReturnsResponse(TopUpBalanceRequest req,
                                                                    UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenReturn(user);
        doNothing().when(kafkaEventPublisher)
                .sendUserBalanceCreditedEvent(anyString(), any(UserBalanceCreditedEvent.class));

        // Act
        UserBalanceResponse response = userService.topUpUserBalance(req);

        // Assert
        assertThat(response).isNotNull();
        assertThat(response.name()).isEqualTo(user.getName());
        assertThat(response.balance()).isEqualTo(new BigDecimal("150.00"));

        verify(userRepository, times(1)).save(user);
        verify(kafkaEventPublisher, times(1))
                .sendUserBalanceCreditedEvent(anyString(), any(UserBalanceCreditedEvent.class));
    }

    private static Stream<Arguments> topUpUserBalance() {
        return Stream.of(
                Arguments.of(
                        new TopUpBalanceRequest(UUID.randomUUID(), new BigDecimal("50.00")),
                        UserEntity.builder()
                                .id(UUID.randomUUID())
                                .username("john_doe")
                                .name("John")
                                .balance(new BigDecimal("100.00"))
                                .build()
                )
        );
    }

    @Test
    void cancelTopUpUserBalance_UserNotFound_ThrowsUserNotFoundException() {
        // Arrange
        UserBalanceRequest req = new UserBalanceRequest(UUID.randomUUID(), new BigDecimal("50.00"));
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(UserNotFoundException.class, () -> userService.cancelTopUpUserBalance(req));
    }

    @ParameterizedTest @MethodSource("userBalanceProvider")
    void cancelTopUpUserBalance_RepositorySaveFails_PropagatesDataAccessException(UserBalanceRequest req,
                                                                                  UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> userService.cancelTopUpUserBalance(req));
    }

    @ParameterizedTest @MethodSource("userBalanceProvider")
    void cancelTopUpUserBalance_ValidRequest_UpdatesBalance(UserBalanceRequest req, UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenReturn(user);

        // Act
        userService.cancelTopUpUserBalance(req);

        // Assert
        assertThat(user.getBalance()).isEqualTo(new BigDecimal("50.00"));
        verify(userRepository, times(1)).save(user);
    }

    @Test
    void debitUserBalance_UserNotFound_ThrowsUserNotFoundException() {
        // Arrange
        UserBalanceRequest req = new UserBalanceRequest(UUID.randomUUID(), new BigDecimal("50.00"));
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(UserNotFoundException.class, () -> userService.debitUserBalance(req));
    }

    @Test
    void debitUserBalance_InsufficientBalance_ThrowsInsufficientBalanceException() {
        // Arrange
        UserBalanceRequest req = new UserBalanceRequest(UUID.randomUUID(), new BigDecimal("150.00"));
        UserEntity user = UserEntity.builder().id(UUID.randomUUID()).balance(new BigDecimal("100.00")).build();
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));

        // Act & Assert
        assertThrows(InsufficientBalanceException.class, () -> userService.debitUserBalance(req));
        verify(userRepository, never()).save(any(UserEntity.class));
    }

    @ParameterizedTest @MethodSource("userBalanceProvider")
    void debitUserBalance_RepositorySaveFails_PropagatesDataAccessException(UserBalanceRequest req,
                                                                            UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> userService.debitUserBalance(req));
    }

    @ParameterizedTest @MethodSource("userBalanceProvider")
    void debitUserBalance_ValidRequest_UpdatesBalance(UserBalanceRequest req, UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenReturn(user);

        // Act
        userService.debitUserBalance(req);

        // Assert
        assertThat(user.getBalance()).isEqualTo(new BigDecimal("50.00"));
        verify(userRepository, times(1)).save(user);
    }

    @Test
    void creditUserBalance_UserNotFound_ThrowsUserNotFoundException() {
        // Arrange
        UserBalanceRequest req = new UserBalanceRequest(UUID.randomUUID(), new BigDecimal("50.00"));
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(UserNotFoundException.class, () -> userService.creditUserBalance(req));
    }

    @ParameterizedTest @MethodSource("userBalanceProvider")
    void creditUserBalance_RepositorySaveFails_PropagatesDataAccessException(UserBalanceRequest req,
                                                                             UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class, () -> userService.creditUserBalance(req));
    }

    @ParameterizedTest @MethodSource("userBalanceProvider")
    void creditUserBalance_ValidRequest_UpdatesBalance(UserBalanceRequest req,
                                                       UserEntity user) {
        // Arrange
        when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(user));
        when(userRepository.save(any(UserEntity.class))).thenReturn(user);

        // Act
        userService.creditUserBalance(req);

        // Assert
        assertThat(user.getBalance()).isEqualTo(new BigDecimal("150.00"));
        verify(userRepository, times(1)).save(user);
    }

    private static Stream<Arguments> userBalanceProvider() {
        return Stream.of(
                Arguments.of(
                        new UserBalanceRequest(UUID.randomUUID(), new BigDecimal("50.00")),
                        UserEntity.builder()
                                .id(UUID.randomUUID())
                                .username("john_doe")
                                .name("John")
                                .balance(new BigDecimal("100.00"))
                                .build()
                )
        );
    }
}
