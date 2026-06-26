package by.pressf.orderms.unit.dao.repository;

import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.OrderRepository;
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

@DataJpaTest(properties = {
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.show-sql=true",
        "spring.jpa.properties.hibernate.format_sql=true",
        "spring.liquibase.enabled=false"
})
public class OrderRepositoryUnitTests {
    @Autowired
    private OrderRepository orderRepository;
    @Autowired
    private TestEntityManager entityManager;

    @BeforeEach
    public void init() {
        orderRepository.deleteAll();
    }

    @Test
    void save_ValidEntity_ReturnSavedEntity() {
        // Arrange
        OrderEntity entity = OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build();

        // Act
        orderRepository.saveAndFlush(entity);
        entityManager.clear();

        // Assert
        OrderEntity savedEntity = orderRepository.findById(entity.getId()).orElse(null);

        assertThat(savedEntity).isNotNull();
        assertThat(savedEntity.getUserId()).isEqualTo(entity.getUserId());
        assertThat(savedEntity.getProductId()).isEqualTo(entity.getProductId());
        assertThat(savedEntity.getQuantity()).isEqualTo(entity.getQuantity());
        assertThat(savedEntity.getStatus()).isEqualTo(entity.getStatus());
    }

    @ParameterizedTest @MethodSource("save_NullArgument")
    void save_NullArgument_ThrowsException(OrderEntity entity) {
        // Arrange & Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> orderRepository.saveAndFlush(entity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_NullArgument() {
        return Stream.of(
                Arguments.of(new OrderEntity(null, null, UUID.randomUUID(),
                        1, OrderStatus.CREATED)),
                Arguments.of(new OrderEntity(null, UUID.randomUUID(), null,
                        1, OrderStatus.CREATED)),
                Arguments.of(new OrderEntity(null, UUID.randomUUID(), UUID.randomUUID(),
                        null, OrderStatus.CREATED)),
                Arguments.of(new OrderEntity(null, UUID.randomUUID(), UUID.randomUUID(),
                        1, null))
        );
    }

    @Test
    void update_UpdatableFields_StayUnchanged() {
        // Arrange
        OrderEntity entity = OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(10)
                .status(OrderStatus.CREATED)
                .build();
        orderRepository.saveAndFlush(entity);
        entityManager.clear();

        // Act
        OrderEntity savedEntity = orderRepository.findById(entity.getId()).orElse(null);
        assertThat(savedEntity).isNotNull();

        savedEntity.setUserId(UUID.randomUUID());
        savedEntity.setProductId(UUID.randomUUID());
        savedEntity.setQuantity(999);
        savedEntity.setStatus(OrderStatus.APPROVED);

        orderRepository.saveAndFlush(savedEntity);
        entityManager.clear();

        // Assert
        OrderEntity changedEntity = orderRepository.findById(entity.getId()).orElse(null);

        assertThat(changedEntity).isNotNull();
        assertThat(changedEntity.getUserId()).isEqualTo(entity.getUserId());
        assertThat(changedEntity.getProductId()).isEqualTo(entity.getProductId());
        assertThat(changedEntity.getQuantity()).isEqualTo(entity.getQuantity());
        assertThat(changedEntity.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }
}
