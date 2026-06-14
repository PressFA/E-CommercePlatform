package by.pressf.emailnotificationms.unit.kafka.listener;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.emailnotificationms.kafka.listener.EmailCommandsListener;
import by.pressf.emailnotificationms.service.EmailService;
import by.pressf.emailnotificationms.service.IdempotencyService;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.mail.MailException;
import org.springframework.mail.MailSendException;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class EmailCommandsListenerUnitTests {
    private @Mock EmailService emailService;
    private @Mock IdempotencyService idempotencyService;
    private @InjectMocks EmailCommandsListener emailCommandsListener;

    @ParameterizedTest @MethodSource("arguments")
    void handleCommand_Success_ReturnsNormally(SendEmailOrderCommand command,
                                               String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(emailService).sendEmail(anyString(), anyString(), anyString());
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> emailCommandsListener.handleCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, times(1)).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleCommand_DuplicateMessage_SwallowsException(SendEmailOrderCommand command,
                                                          String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> emailCommandsListener.handleCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, never()).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleCommand_MailSendError_ThrowsRetryableException(SendEmailOrderCommand command,
                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(MailSendException.class)).when(emailService)
                .sendEmail(anyString(), anyString(), anyString());

        // Act & Assert
        RetryableException ex = assertThrows(RetryableException.class,
                () -> emailCommandsListener.handleCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());

        assertInstanceOf(MailSendException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleCommand_GenericMailError_ThrowsNotRetryableException(SendEmailOrderCommand command,
                                                                    String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(MailException.class)).when(emailService)
                .sendEmail(anyString(), anyString(), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> emailCommandsListener.handleCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());

        assertInstanceOf(MailException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handleCommand_DataAccessError_ThrowsNotRetryableException(SendEmailOrderCommand command,
                                                                   String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(emailService).sendEmail(anyString(), anyString(), anyString());
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> emailCommandsListener.handleCommand(command, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, times(1)).saveIdempotentKey(anyString(), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(new SendEmailOrderCommand("test@mail.com", "title", "text",
                        UUID.randomUUID()), "msg-123-test")
        );
    }
}
