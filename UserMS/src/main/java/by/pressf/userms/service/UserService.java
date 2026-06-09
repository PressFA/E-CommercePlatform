package by.pressf.userms.service;

import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dao.repository.UserRepository;
import by.pressf.userms.dto.internal.UserBalanceRequest;
import by.pressf.userms.dto.internal.UserBalanceResponse;
import by.pressf.userms.dto.internal.UserCreationData;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.publisher.KafkaEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;
    private final KafkaEventPublisher kafkaEventPublisher;

    @Transactional("transactionManager")
    public UUID createUser(@NonNull UserCreationData userData) {
        Objects.requireNonNull(userData, "UserCreationData must not be null");

        UserEntity userEntity = UserEntity.builder()
                .username(userData.username())
                .password(userData.password())
                .name(userData.name())
                .build();
        userRepository.save(userEntity);
        log.info("A new user with the ID {} has been successfully created", userEntity.getId());

        return userEntity.getId();
    }

    @Transactional("transactionManager")
    public UserBalanceResponse topUpUserBalance(@NonNull UserBalanceRequest req) {
        Objects.requireNonNull(req, "UserBalanceRequest must not be null");

        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        user.setBalance(user.getBalance().add(req.amount()));

        userRepository.save(user);
        log.info("The balance of the user with the ID {} has been topped up to {}", req.userId(), req.amount());

        UserBalanceCreditedEvent event = new UserBalanceCreditedEvent(
                user.getId(),
                user.getUsername(),
                req.amount()
        );

        kafkaEventPublisher.sendUserBalanceCreditedEvent(event.userId().toString(), event);

        return new UserBalanceResponse(user.getName(), user.getBalance());
    }

    public void cancelTopUpUserBalance(@NonNull UserBalanceRequest req) {
        Objects.requireNonNull(req, "UserBalanceRequest must not be null");

        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        user.setBalance(user.getBalance().subtract(req.amount()));
        userRepository.save(user);
    }

    public void debitUserBalance(@NonNull UserBalanceRequest req) {
        Objects.requireNonNull(req, "UserBalanceRequest must not be null");

        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        if (req.amount().compareTo(user.getBalance()) > 0) {
            throw new InsufficientBalanceException(req.userId(), user.getBalance(), req.amount());
        }

        user.setBalance(user.getBalance().subtract(req.amount()));
        userRepository.save(user);
    }

    public void creditUserBalance(@NonNull UserBalanceRequest req) {
        Objects.requireNonNull(req, "UserBalanceRequest must not be null");

        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        user.setBalance(user.getBalance().add(req.amount()));

        userRepository.save(user);
    }
}
