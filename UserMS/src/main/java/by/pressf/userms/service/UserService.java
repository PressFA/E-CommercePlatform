package by.pressf.userms.service;

import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dao.repository.UserRepository;
import by.pressf.userms.dto.UserBalanceRequest;
import by.pressf.userms.dto.UserBalanceResponse;
import by.pressf.userms.dto.UserCreationData;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final UserRepository userRepository;

    @Transactional("transactionManager")
    public UUID createUser(UserCreationData userData) {
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
    public UserBalanceResponse topUpUserBalance(UserBalanceRequest req) {
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

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(
                        env.getRequiredProperty("user-payment.events.topic.name"),
                        event.userId().toString(),
                        event
                );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The UserBalanceCreditedEvent message was sent to the user-payment-events topic");

        return new UserBalanceResponse(user.getName(), user.getBalance());
    }

    public void cancelTopUpUserBalance(UserBalanceRequest req) {
        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        user.setBalance(user.getBalance().subtract(req.amount()));
        userRepository.save(user);
    }

    public void debitUserBalance(UserBalanceRequest req) {
        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        if (req.amount().compareTo(user.getBalance()) > 0) {
            throw new InsufficientBalanceException(req.userId(), user.getBalance(), req.amount());
        }

        user.setBalance(user.getBalance().subtract(req.amount()));
        userRepository.save(user);
    }

    public void creditUserBalance(UserBalanceRequest req) {
        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        user.setBalance(user.getBalance().add(req.amount()));

        userRepository.save(user);
    }
}
