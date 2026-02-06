package by.pressf.orderms.unit.dao.repository;

import by.pressf.orderms.dao.entity.OrderHistoryEntity;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.dao.repository.OrderHistoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
public class OrderHistoryRepositoryUnitTests {
    @Autowired
    private OrderHistoryRepository repository;
    @Autowired
    private TestEntityManager entityManager;

    @BeforeEach
    public void init() {
        repository.deleteAll();
    }

    @Test
    void save_ValidEntity_ReturnSavedEntity() {
        // Arrange
        OrderHistoryEntity entity = OrderHistoryEntity.builder()
                .orderId(UUID.randomUUID())
                .status(OrderHistoryStatus.SUCCESS)
                .reason("lots of text")
                .build();

        // Act
        repository.saveAndFlush(entity);

        entityManager.clear();

        // Assert
        OrderHistoryEntity savedEntity = repository.findById(entity.getId()).orElse(null);

        assertThat(savedEntity).isNotNull();
        assertThat(savedEntity.getOrderId()).isEqualTo(entity.getOrderId());
        assertThat(savedEntity.getStatus()).isEqualTo(entity.getStatus());
        assertThat(savedEntity.getCreatedAt()).isEqualTo(entity.getCreatedAt());
        assertThat(savedEntity.getReason()).isEqualTo(entity.getReason());
    }

    @ParameterizedTest @MethodSource("save_NullArgument")
    void save_NullArgument_ThrowsException(OrderHistoryEntity entity) {
        // Arrange
        OrderHistoryEntity saveEntity = OrderHistoryEntity.builder()
                .orderId(entity.getOrderId())
                .status(entity.getStatus())
                .reason(entity.getReason())
                .build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> repository.saveAndFlush(saveEntity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_NullArgument() {
        return Stream.of(
                Arguments.of(new OrderHistoryEntity(null, null, OrderHistoryStatus.SUCCESS, null, "reason")),
                Arguments.of(new OrderHistoryEntity(null, UUID.randomUUID(), null, null, "reason")),
                Arguments.of(new OrderHistoryEntity(null, UUID.randomUUID(), OrderHistoryStatus.SUCCESS, null, null))
        );
    }

    @Test
    void update_UpdatableFields_StayUnchanged() {
        // Arrange
        OrderHistoryEntity entity = OrderHistoryEntity.builder()
                .orderId(UUID.randomUUID())
                .status(OrderHistoryStatus.SUCCESS)
                .reason("lots of text")
                .build();
        repository.saveAndFlush(entity);

        entityManager.clear();

        // Act
        OrderHistoryEntity savedEntity = repository.findById(entity.getId()).orElse(null);
        assertThat(savedEntity).isNotNull();

        savedEntity.setOrderId(null);
        savedEntity.setStatus(OrderHistoryStatus.FAIL);
        savedEntity.setCreatedAt(null);
        savedEntity.setReason("new reason");

        repository.saveAndFlush(savedEntity);

        entityManager.clear();

        // Assert
        OrderHistoryEntity changedEntity = repository.findById(entity.getId()).orElse(null);

        assertThat(changedEntity).isNotNull();
        assertThat(changedEntity.getOrderId()).isEqualTo(entity.getOrderId());
        assertThat(changedEntity.getStatus()).isEqualTo(entity.getStatus());
        assertThat(changedEntity.getCreatedAt()).isEqualTo(entity.getCreatedAt());
        assertThat(changedEntity.getReason()).isEqualTo(entity.getReason());
    }
}
