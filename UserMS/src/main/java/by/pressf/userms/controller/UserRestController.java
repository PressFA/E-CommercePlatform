package by.pressf.userms.controller;

import by.pressf.userms.dto.CreateUserRequest;
import by.pressf.userms.dto.UserCreationData;
import by.pressf.userms.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/user")
public class UserRestController {
    private final UserService userService;

    @PostMapping
    public ResponseEntity<?> createUser(@RequestBody CreateUserRequest req) {
        log.info("A request was received to create a user named {}", req.name());

        UserCreationData userCreationData = new UserCreationData(
                req.name(),
                req.balance()
        );

        UUID userId = userService.createUser(userCreationData);

        return ResponseEntity.status(HttpStatus.CREATED).body(userId);
    }
}
