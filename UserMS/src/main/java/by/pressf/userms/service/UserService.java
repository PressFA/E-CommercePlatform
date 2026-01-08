package by.pressf.userms.service;

import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dao.repository.UserRepository;
import by.pressf.userms.dto.CreditUserBalanceRequest;
import by.pressf.userms.dto.DebitUserBalanceRequest;
import by.pressf.userms.dto.UserCreationData;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;

    @Transactional("transactionManager")
    public UUID createUser(UserCreationData creationData) {
        UserEntity userEntity = UserEntity.builder()
                .name(creationData.name())
                .balance(creationData.balance())
                .build();
        userRepository.save(userEntity);
        log.info("A new user with the ID {} has been successfully created", userEntity.getId());

        return userEntity.getId();
    }

    public void debitUserBalance(DebitUserBalanceRequest req) {
        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        if (req.amount().compareTo(user.getBalance()) > 0) {
            throw new InsufficientBalanceException(req.userId(), user.getBalance(), req.amount());
        }

        user.setBalance(user.getBalance().subtract(req.amount()));
        userRepository.save(user);
    }

    public void creditUserBalance(CreditUserBalanceRequest req) {
        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        user.setBalance(user.getBalance().add(req.amount()));

        userRepository.save(user);
    }
}
