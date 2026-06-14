package by.pressf.emailnotificationms.unit.service;

import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.emailnotificationms.dao.entity.EventEntity;
import by.pressf.emailnotificationms.dao.repository.EventRepository;
import by.pressf.emailnotificationms.service.IdempotencyService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IdempotencyServiceUnitTests {
    private @Mock EventRepository eventRepository;
    private @InjectMocks IdempotencyService idempotencyService;

    @Test
    void idempotenceCheck_MessageAlreadyProcessed_ThrowsDuplicateMessageException() {
        // Arrange
        when(eventRepository.findByMessageId(anyString())).thenReturn(mock(EventEntity.class));

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> idempotencyService.idempotenceCheck(String.valueOf(UUID.randomUUID()), "Event"));

        verify(eventRepository, times(1)).findByMessageId(anyString());
    }

    @Test
    void idempotenceCheck_MessageNotProcessed_CompletesSuccessfully() {
        // Arrange
        when(eventRepository.findByMessageId(anyString())).thenReturn(null);

        // Act
        idempotencyService.idempotenceCheck(String.valueOf(UUID.randomUUID()), "Event");

        // Assert
        verify(eventRepository, times(1)).findByMessageId(anyString());
    }

    @Test
    void saveIdempotentKey_RepositorySaveFails_PropagatesDataAccessException() {
        // Arrange
        when(eventRepository.save(any(EventEntity.class))).thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> idempotencyService.saveIdempotentKey(String.valueOf(UUID.randomUUID()), "Event"));

        verify(eventRepository, times(1)).save(any(EventEntity.class));
    }

    @Test
    void saveIdempotentKey_ValidData_SavesKey() {
        // Arrange
        when(eventRepository.save(any(EventEntity.class))).thenReturn(mock(EventEntity.class));

        // Act
        idempotencyService.saveIdempotentKey(String.valueOf(UUID.randomUUID()), "Event");

        // Assert
        verify(eventRepository, times(1)).save(any(EventEntity.class));
    }
}
