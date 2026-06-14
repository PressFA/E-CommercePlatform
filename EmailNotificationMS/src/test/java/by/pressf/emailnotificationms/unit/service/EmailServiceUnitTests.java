package by.pressf.emailnotificationms.unit.service;

import by.pressf.emailnotificationms.service.EmailService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mail.MailException;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class EmailServiceUnitTests {
    private @Mock JavaMailSender mailSender;
    private @InjectMocks EmailService emailService;

    @ParameterizedTest @MethodSource("createThrowable")
    void sendEmail_MailSenderFails_PropagatesException(Throwable ex) {
        // Arrange
        doThrow(ex).when(mailSender).send(any(SimpleMailMessage.class));

        // Act & Assert
        assertThrows(MailException.class,
                () -> emailService.sendEmail("to", "subject", "body"));

        verify(mailSender, times(1)).send(any(SimpleMailMessage.class));
    }

    private static Stream<Arguments> createThrowable() {
        return Stream.of(
                Arguments.of(mock(MailSendException.class)),
                Arguments.of(mock(MailException.class))
        );
    }

    @Test
    void sendEmail_ValidParameters_SendsCorrectMessage() {
        // Arrange
        String to = "user@test.com";
        String subject = "Welcome!";
        String body = "Glad to see you.";

        // Act
        emailService.sendEmail(to, subject, body);

        // Assert
        ArgumentCaptor<SimpleMailMessage> messageCaptor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(messageCaptor.capture());

        assertThat(messageCaptor.getValue().getTo()).containsExactly(to);
        assertThat(messageCaptor.getValue().getSubject()).isEqualTo(subject);
        assertThat(messageCaptor.getValue().getText()).isEqualTo(body);

        verify(mailSender, times(1)).send(any(SimpleMailMessage.class));
    }
}
