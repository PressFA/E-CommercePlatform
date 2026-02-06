package by.pressf.productms.unit.dao.repository;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.repository.ProductRepository;
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

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
class ProductRepositoryUnitTests {
    @Autowired
    private ProductRepository productRepository;
    @Autowired
    private TestEntityManager entityManager;

    @BeforeEach
    public void init() {
        productRepository.deleteAll();
    }

    @Test
    void save_ValidEntity_ReturnSavedEntity() {
        // Arrange
        ProductEntity product = ProductEntity.builder()
                .name("Gaming Mouse")
                .quantity(10)
                .price(new BigDecimal("49.99"))
                .build();

        // Act
        productRepository.saveAndFlush(product);
        entityManager.clear();

        // Assert
        ProductEntity savedProduct = productRepository.findById(product.getId()).orElse(null);

        assertThat(savedProduct).isNotNull();
        assertThat(savedProduct.getName()).isEqualTo(product.getName());
        assertThat(savedProduct.getQuantity()).isEqualTo(product.getQuantity());
        assertThat(savedProduct.getPrice()).isEqualByComparingTo(product.getPrice());
        assertThat(savedProduct.getVersion()).isEqualTo(product.getVersion());
    }

    @Test
    void save_DuplicateProductName_ThrowsException() {
        // Arrange
        String name = "Unique Product";
        ProductEntity product1 = ProductEntity.builder()
                .name(name)
                .quantity(1)
                .price(BigDecimal.TEN)
                .build();
        ProductEntity product2 = ProductEntity.builder()
                .name(name)
                .quantity(5)
                .price(BigDecimal.ONE)
                .build();

        productRepository.saveAndFlush(product1);
        entityManager.clear();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> productRepository.saveAndFlush(product2));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void save_InvalidLength_ThrowsException() {
        // Arrange
        ProductEntity product = ProductEntity.builder()
                .name("This name is definitely longer than fifty characters so it should fail")
                .quantity(10)
                .price(BigDecimal.ONE)
                .build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> productRepository.saveAndFlush(product));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    @ParameterizedTest @MethodSource("save_NullArgument")
    void save_NullArgument_ThrowsException(ProductEntity productEntity) {
        // Arrange
        ProductEntity entityToSave = ProductEntity.builder()
                .name(productEntity.getName())
                .quantity(productEntity.getQuantity())
                .price(productEntity.getPrice())
                .build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> productRepository.saveAndFlush(entityToSave));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_NullArgument() {
        return Stream.of(
                Arguments.of(new ProductEntity(null, null, 5, new BigDecimal("9.99"), null)),
                Arguments.of(new ProductEntity(null, "product name", null, new BigDecimal("9.99"), null)),
                Arguments.of(new ProductEntity(null, "product name", 5, null, null))
        );
    }

    @Test
    void update_UpdatableFields_UpdatableProduct() {
        // Arrange
        ProductEntity product = ProductEntity.builder()
                .name("Original Name")
                .quantity(10)
                .price(new BigDecimal("100.00"))
                .build();
        productRepository.saveAndFlush(product);
        entityManager.clear();

        // Act
        ProductEntity toUpdate = productRepository.findById(product.getId()).orElse(null);
        assertThat(toUpdate).isNotNull();

        toUpdate.setName(null);
        toUpdate.setQuantity(20);
        toUpdate.setPrice(new BigDecimal("150.00"));

        productRepository.saveAndFlush(toUpdate);
        entityManager.clear();

        // Assert
        ProductEntity result = productRepository.findById(product.getId()).orElse(null);

        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo(product.getName());
        assertThat(result.getQuantity()).isEqualTo(toUpdate.getQuantity());
        assertThat(result.getPrice()).isEqualByComparingTo(toUpdate.getPrice());
        assertThat(result.getVersion()).isEqualTo(product.getVersion() + 1);
    }
}
