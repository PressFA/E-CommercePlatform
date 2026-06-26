package by.pressf.emailnotificationms.unit.kafka.listener;

import by.pressf.core.dto.choreography.events.BalanceTopUpCompletedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.emailnotificationms.kafka.listener.REmailWPaymentEventsListener;
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

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class REmailWPaymentEventsListenerUnitTests {
    private @Mock EmailService emailService;
    private @Mock IdempotencyService idempotencyService;
    private @InjectMocks REmailWPaymentEventsListener listener;

    @ParameterizedTest @MethodSource("arguments")
    void handle_Success_ReturnsNormally(BalanceTopUpCompletedEvent event,
                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(emailService).sendEmail(anyString(), anyString(), anyString());
        doNothing().when(idempotencyService).saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, times(1)).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handle_DuplicateMessage_SwallowsException(BalanceTopUpCompletedEvent event,
                                                   String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> listener.handle(event, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, never()).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handle_MailSendError_ThrowsRetryableException(BalanceTopUpCompletedEvent event,
                                                       String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(MailSendException.class)).when(emailService)
                .sendEmail(anyString(), anyString(), anyString());

        // Act & Assert
        RetryableException ex = assertThrows(RetryableException.class,
                () -> listener.handle(event, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());

        assertInstanceOf(MailSendException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handle_GenericMailError_ThrowsNotRetryableException(BalanceTopUpCompletedEvent event,
                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doThrow(mock(MailException.class)).when(emailService)
                .sendEmail(anyString(), anyString(), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, never()).saveIdempotentKey(anyString(), anyString());

        assertInstanceOf(MailException.class, ex.getCause());
    }

    @ParameterizedTest @MethodSource("arguments")
    void handle_DataAccessError_ThrowsNotRetryableException(BalanceTopUpCompletedEvent event,
                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService).idempotenceCheck(anyString(), anyString());
        doNothing().when(emailService).sendEmail(anyString(), anyString(), anyString());
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        NotRetryableException ex = assertThrows(NotRetryableException.class,
                () -> listener.handle(event, messageId));

        verify(idempotencyService, times(1)).idempotenceCheck(anyString(), anyString());
        verify(emailService, times(1)).sendEmail(anyString(), anyString(), anyString());
        verify(idempotencyService, times(1)).saveIdempotentKey(anyString(), anyString());

        assertInstanceOf(DataAccessException.class, ex.getCause());
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(new BalanceTopUpCompletedEvent("test@mail.com", "title",
                        "text"), "msg-123-test")
        );
    }
}
