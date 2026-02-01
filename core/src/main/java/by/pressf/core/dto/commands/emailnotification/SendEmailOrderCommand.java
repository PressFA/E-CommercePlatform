package by.pressf.core.dto.commands.emailnotification;

import java.util.UUID;

public record SendEmailOrderCommand(String email,
                                    String subject,
                                    String body,
                                    UUID orderId) {
}
