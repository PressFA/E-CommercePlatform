package by.pressf.userms.controller;

import by.pressf.userms.dto.incoming.CreateUserRequest;
import by.pressf.userms.dto.incoming.TopUpBalanceRequest;
import by.pressf.userms.dto.internal.UserBalanceResponse;
import by.pressf.userms.service.UserService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/user")
public class UserRestController {
    private final UserService userService;

    @PostMapping
    public ResponseEntity<?> createUser(@RequestBody @Valid CreateUserRequest request) {
        log.info("A request was received to create a user named {}", request.name());

        UUID userId = userService.createUser(request);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("userId", userId));
    }

    @PatchMapping("/balance/top-up")
    public ResponseEntity<?> topUpUsersBalance(@RequestBody @Valid TopUpBalanceRequest request) {
        log.info("A request was received to top up the user's balance with the ID {}", request.userId());

        UserBalanceResponse resp = userService.topUpUserBalance(request);

        return ResponseEntity.status(HttpStatus.OK).body(resp);
    }
}
