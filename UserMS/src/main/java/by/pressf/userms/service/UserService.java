package by.pressf.userms.service;

import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dao.repository.UserRepository;
import by.pressf.userms.dto.DebitUserBalanceRequest;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;

    public void debitUserBalance(DebitUserBalanceRequest req) {
        UserEntity user = userRepository.findById(req.userId())
                .orElseThrow(() -> new UserNotFoundException(req.userId()));

        if (req.amount().compareTo(user.getBalance()) > 0) {
            throw new InsufficientBalanceException(req.userId(), user.getBalance(), req.amount());
        }

        user.setBalance(user.getBalance().subtract(req.amount()));
        userRepository.save(user);
    }
}
