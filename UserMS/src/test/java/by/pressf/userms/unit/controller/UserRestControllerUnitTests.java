package by.pressf.userms.unit.controller;

import by.pressf.userms.controller.UserRestController;
import by.pressf.userms.dto.incoming.CreateUserRequest;
import by.pressf.userms.dto.incoming.TopUpBalanceRequest;
import by.pressf.userms.dto.internal.UserBalanceResponse;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.service.UserService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(UserRestController.class)
public class UserRestControllerUnitTests {
    @MockitoBean
    private UserService userService;
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper mapper;

    @ParameterizedTest @MethodSource("provideInvalidUsers")
    void createUser_InvalidData_ReturnsBadRequest(CreateUserRequest invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(post("/api/v1/user")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(userService);
    }

    private static Stream<Arguments> provideInvalidUsers() {
        return Stream.of(
                Arguments.of(new CreateUserRequest("", "validpass", "validname")),
                Arguments.of(new CreateUserRequest("invalid-email", "validpass", "validname")),
                Arguments.of(new CreateUserRequest("test@test.com", "", "validname")),
                Arguments.of(new CreateUserRequest("test@test.com", "123", "validname")),
                Arguments.of(new CreateUserRequest("test@test.com", "verylongpassword123456", "validname")),
                Arguments.of(new CreateUserRequest("test@test.com", "validpass", "")),
                Arguments.of(new CreateUserRequest("test@test.com", "validpass", "abc")),
                Arguments.of(new CreateUserRequest("test@test.com", "validpass", "verylongname1234567890"))
        );
    }

    @Test
    void createUser_DatabaseError_ReturnsInternalServerError() throws Exception {
        // Arrange
        CreateUserRequest request =
                new CreateUserRequest("test@test.com", "validpass", "validname");

        when(userService.createUser(any(CreateUserRequest.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/user")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(userService, times(1)).createUser(any(CreateUserRequest.class));
    }

    @Test
    void createUser_ValidData_ReturnsCreated() throws Exception {
        // Arrange
        CreateUserRequest request =
                new CreateUserRequest("test@test.com", "validpass", "validname");

        when(userService.createUser(any(CreateUserRequest.class))).thenReturn(UUID.randomUUID());

        // Act & Assert
        mockMvc.perform(post("/api/v1/user")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isCreated(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.userId").exists()
                );

        verify(userService, times(1)).createUser(any(CreateUserRequest.class));
    }

    @ParameterizedTest @MethodSource("provideInvalidBalanceRequests")
    void topUpBalance_InvalidData_ReturnsBadRequest(TopUpBalanceRequest invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(patch("/api/v1/user/balance/top-up")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(userService);
    }

    private static Stream<Arguments> provideInvalidBalanceRequests() {
        UUID id = UUID.randomUUID();

        return Stream.of(
                Arguments.of(new TopUpBalanceRequest(null, new BigDecimal("100"))),
                Arguments.of(new TopUpBalanceRequest(id, null)),
                Arguments.of(new TopUpBalanceRequest(id, new BigDecimal("4.99"))),
                Arguments.of(new TopUpBalanceRequest(id, new BigDecimal("10000.01")))
        );
    }

    @Test
    void topUpBalance_UserNotFound_ReturnsNotFound() throws Exception {
        // Arrange
        UUID userId = UUID.randomUUID();
        TopUpBalanceRequest request = new TopUpBalanceRequest(userId, new BigDecimal("100"));

        when(userService.topUpUserBalance(any(TopUpBalanceRequest.class)))
                .thenThrow(new UserNotFoundException(userId));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/user/balance/top-up")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isNotFound(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("User with id " + userId + " not found")
                );

        verify(userService, times(1)).topUpUserBalance(any(TopUpBalanceRequest.class));
    }

    @Test
    void topUpBalance_OptimisticLockingFailure_ReturnsConflict() throws Exception {
        // Arrange
        TopUpBalanceRequest request = new TopUpBalanceRequest(UUID.randomUUID(), new BigDecimal("100"));

        when(userService.topUpUserBalance(any(TopUpBalanceRequest.class)))
                .thenThrow(mock(OptimisticLockingFailureException.class));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/user/balance/top-up")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isConflict(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Data was modified by another user. Please refresh.")
                );

        verify(userService, times(1)).topUpUserBalance(any(TopUpBalanceRequest.class));
    }

    @Test
    void topUpBalance_ValidData_ReturnsUpdatedBalance() throws Exception {
        // Arrange
        TopUpBalanceRequest request = new TopUpBalanceRequest(UUID.randomUUID(), new BigDecimal("100"));
        UserBalanceResponse response = new UserBalanceResponse("John Doe", new BigDecimal("200"));

        when(userService.topUpUserBalance(any(TopUpBalanceRequest.class))).thenReturn(response);

        // Act & Assert
        mockMvc.perform(patch("/api/v1/user/balance/top-up")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isOk(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.name").value("John Doe"),
                        jsonPath("$.balance").value(200)
                );

        verify(userService, times(1)).topUpUserBalance(any(TopUpBalanceRequest.class));
    }
}
