package by.pressf.userms.controller;

import by.pressf.userms.dto.incoming.CreateUserRequest;
import by.pressf.userms.dto.incoming.TopUpBalanceRequest;
import by.pressf.userms.dto.internal.UserBalanceRequest;
import by.pressf.userms.dto.internal.UserBalanceResponse;
import by.pressf.userms.dto.internal.UserCreationData;
import by.pressf.userms.service.UserService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/user")
public class UserRestController {
    private final UserService userService;

    @PostMapping
    public ResponseEntity<?> createUser(@RequestBody @Valid CreateUserRequest req) {
        log.info("A request was received to create a user named {}", req.name());

        UserCreationData userCreationData = new UserCreationData(
                req.username(),
                req.password(),
                req.name()
        );

        UUID userId = userService.createUser(userCreationData);

        return ResponseEntity.status(HttpStatus.CREATED).body(userId);
    }

    @PatchMapping("/balance/top-up")
    public ResponseEntity<?> topUpUsersBalance(@RequestBody @Valid TopUpBalanceRequest req) {
        log.info("A request was received to top up the user's balance with the ID {}", req.userId());

        UserBalanceRequest userBalanceRequest = new UserBalanceRequest(req.userId(), req.amount());

        UserBalanceResponse resp = userService.topUpUserBalance(userBalanceRequest);

        return ResponseEntity.status(HttpStatus.OK).body(resp);
    }
}
