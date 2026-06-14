package by.pressf.orderms.unit.service;

import by.pressf.orderms.dao.entity.OrderHistoryEntity;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.dao.repository.OrderHistoryRepository;
import by.pressf.orderms.service.OrderHistoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OrderHistoryServiceUnitTests {
    private @Mock OrderHistoryRepository orderHistoryRepository;
    private @InjectMocks OrderHistoryService orderHistoryService;

    @ParameterizedTest
    @EnumSource(OrderHistoryStatus.class)
    void createHistoryLog_ValidArguments_SavesEntity(OrderHistoryStatus status) {
        // Arrange & Act
        orderHistoryService.createHistoryLog(UUID.randomUUID(), status, "reason");

        // Assert
        verify(orderHistoryRepository, times(1)).save(any(OrderHistoryEntity.class));
    }

    @Test
    void createHistoryLog_RepositoryThrowsException_ThrowsException() {
        // Arrange
        when(orderHistoryRepository.save(any(OrderHistoryEntity.class)))
                .thenThrow(new DataIntegrityViolationException(null));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> orderHistoryService.createHistoryLog(UUID.randomUUID(), OrderHistoryStatus.SUCCESS, "reason"));

        verify(orderHistoryRepository).save(any(OrderHistoryEntity.class));
    }
}
