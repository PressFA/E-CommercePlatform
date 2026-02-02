package by.pressf.userms.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateUserRequest(
        @NotBlank(message = "The username(email) field is empty!")
        @Email(message = "Invalid email address!")
        String username,
        @NotBlank(message = "The password field is empty!")
        @Size(min = 8, max = 16, message = "The password must be between 8 and 16 characters long!")
        String password,
        @NotBlank(message = "The name field is empty!")
        @Size(min = 6, max = 16, message = "The allowed name size is from 6 to 16 characters!")
        String name) {
}
