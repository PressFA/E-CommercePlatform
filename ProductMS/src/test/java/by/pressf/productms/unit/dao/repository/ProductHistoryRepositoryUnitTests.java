package by.pressf.productms.unit.dao.repository;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.entity.ProductHistoryEntity;
import by.pressf.productms.dao.entity.status.ProductStatus;
import by.pressf.productms.dao.repository.ProductHistoryRepository;
import by.pressf.productms.dao.repository.ProductRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProductHistoryRepositoryUnitTests {
    @Autowired
    private ProductHistoryRepository repository;
    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private ProductRepository productRepository;
    private ProductEntity product;

    @BeforeAll
    public void initProduct() {
        product = ProductEntity.builder()
                .name("AKM")
                .quantity(60)
                .price(new BigDecimal("1959.45"))
                .build();
        product = productRepository.saveAndFlush(product);
    }

    @BeforeEach
    public void init() {
        repository.deleteAll();
    }

    @ParameterizedTest @MethodSource("findByOrderId")
    void findByOrderId_HistoryExists_ReturnEntity(List<ProductHistoryEntity> history) {
        // Arrange
        UUID orderId = history.getLast().getOrderId();
        repository.saveAllAndFlush(history.stream()
                .peek((entity) -> entity.setProduct(product))
                .collect(Collectors.toList())
        );
        entityManager.clear();

        // Act
        ProductHistoryEntity result = repository.findByOrderId(orderId);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getOrderId()).isEqualTo(history.getLast().getOrderId());
    }

    @ParameterizedTest @MethodSource("findByOrderId")
    void findByOrderId_HistoryNotFound_ReturnNull(List<ProductHistoryEntity> history) {
        // Arrange
        repository.saveAllAndFlush(history.stream()
                .peek((entity) -> entity.setProduct(product))
                .collect(Collectors.toList())
        );
        entityManager.clear();

        // Act
        ProductHistoryEntity result = repository.findByOrderId(UUID.randomUUID());

        // Assert
        assertThat(result).isNull();
    }

    @ParameterizedTest @MethodSource("findByOrderId")
    void findByOrderId_NullArgument_ReturnNull(List<ProductHistoryEntity> history) {
        // Arrange
        repository.saveAllAndFlush(history.stream()
                .peek((entity) -> entity.setProduct(product))
                .collect(Collectors.toList())
        );
        entityManager.clear();

        // Act
        ProductHistoryEntity result = repository.findByOrderId(null);

        // Assert
        assertThat(result).isNull();
    }

    private static Stream<Arguments> findByOrderId() {
        return Stream.of(
                Arguments.of(List.of(
                        ProductHistoryEntity.builder().orderId(UUID.randomUUID()).product(null).quantity(10)
                                .status(ProductStatus.RESERVED).build()
                )),
                Arguments.of(List.of(
                        ProductHistoryEntity.builder().orderId(UUID.randomUUID()).product(null).quantity(10)
                                .status(ProductStatus.RESERVED).build(),
                        ProductHistoryEntity.builder().orderId(UUID.randomUUID()).product(null).quantity(15)
                                .status(ProductStatus.RESERVED).build(),
                        ProductHistoryEntity.builder().orderId(UUID.randomUUID()).product(null).quantity(5)
                                .status(ProductStatus.RESERVED).build()
                ))
        );
    }

    @Test
    void save_ValidEntity_ReturnSavedEntity() {
        // Arrange
        ProductHistoryEntity history = ProductHistoryEntity.builder()
                .orderId(UUID.randomUUID())
                .product(product)
                .quantity(10)
                .status(ProductStatus.RESERVED)
                .build();

        // Act
        repository.saveAndFlush(history);
        entityManager.clear();

        // Assert
        ProductHistoryEntity saved = repository.findById(history.getId()).orElse(null);

        assertThat(saved).isNotNull();
        assertThat(saved.getOrderId()).isEqualTo(history.getOrderId());
        assertThat(saved.getProduct()).isEqualTo(product);
        assertThat(saved.getQuantity()).isEqualTo(history.getQuantity());
        assertThat(saved.getCreatedAt()).isEqualTo(history.getCreatedAt());
        assertThat(saved.getUpdatedAt()).isEqualTo(history.getUpdatedAt());
        assertThat(saved.getStatus()).isEqualTo(history.getStatus());
    }

    @ParameterizedTest @MethodSource("save_NullArgument")
    void save_NullArgument_ThrowsException(ProductHistoryEntity entity) {
        // Arrange
        ProductHistoryEntity saveEntity = ProductHistoryEntity.builder()
                .orderId(entity.getOrderId())
                .product(entity.getProduct() == null ? null : product)
                .quantity(entity.getQuantity())
                .status(entity.getStatus())
                .build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> repository.saveAndFlush(saveEntity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_NullArgument() {
        return Stream.of(
                Arguments.of(new ProductHistoryEntity(null, null, ProductEntity.builder().build(), 10, null, null, ProductStatus.RESERVED)),
                Arguments.of(new ProductHistoryEntity(null, UUID.randomUUID(), null, 10, null, null, ProductStatus.RESERVED)),
                Arguments.of(new ProductHistoryEntity(null, UUID.randomUUID(), ProductEntity.builder().build(), null, null, null, ProductStatus.RESERVED)),
                Arguments.of(new ProductHistoryEntity(null, UUID.randomUUID(), ProductEntity.builder().build(), 10, null, null, null))
        );
    }

    @Test
    void update_UpdatableFields_StayUnchanged() {
        // Arrange
        ProductHistoryEntity history = ProductHistoryEntity.builder()
                .orderId(UUID.randomUUID())
                .product(product)
                .quantity(5)
                .status(ProductStatus.RESERVED)
                .build();
        repository.saveAndFlush(history);
        entityManager.clear();

        // Act
        ProductHistoryEntity toUpdate = repository.findById(history.getId()).orElse(null);
        assertThat(toUpdate).isNotNull();

        toUpdate.setOrderId(null);
        toUpdate.setProduct(null);
        toUpdate.setQuantity(null);
        toUpdate.setUpdatedAt(LocalDateTime.now().truncatedTo(ChronoUnit.MICROS));
        toUpdate.setStatus(ProductStatus.CANCELLED);

        repository.saveAndFlush(toUpdate);
        entityManager.clear();

        // Assert
        ProductHistoryEntity result = repository.findById(history.getId()).orElse(null);

        assertThat(result).isNotNull();
        assertThat(result.getOrderId()).isEqualTo(history.getOrderId());
        assertThat(result.getProduct()).isEqualTo(history.getProduct());
        assertThat(result.getQuantity()).isEqualTo(history.getQuantity());
        assertThat(result.getUpdatedAt()).isEqualTo(toUpdate.getUpdatedAt());
        assertThat(result.getStatus()).isEqualTo(toUpdate.getStatus());
    }
}
