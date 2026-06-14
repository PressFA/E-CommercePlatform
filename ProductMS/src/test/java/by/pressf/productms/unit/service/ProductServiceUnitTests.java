package by.pressf.productms.unit.service;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.repository.ProductRepository;
import by.pressf.productms.dto.incoming.CreateProductRequest;
import by.pressf.productms.dto.incoming.PatchProductRequest;
import by.pressf.productms.dto.internal.ProductData;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.exception.ProductOverflowException;
import by.pressf.productms.service.ProductService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ProductServiceUnitTests {
    private @Mock ProductRepository productRepository;
    private @InjectMocks ProductService productService;

    @Test
    void createProduct_RepositoryThrowsException_PropagatesException() {
        // Arrange
        CreateProductRequest creationData =
                new CreateProductRequest("seedlings", 10, new BigDecimal("100.00"));

        when(productRepository.save(any(ProductEntity.class)))
                .thenThrow(new DataIntegrityViolationException(null));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> productService.createProduct(creationData));

        verify(productRepository, times(1)).save(any(ProductEntity.class));
    }

    @Test
    void createProduct_ValidData_ReturnsGeneratedId() {
        // Arrange
        CreateProductRequest creationData =
                new CreateProductRequest("seedlings", 10, new BigDecimal("100.00"));

        doAnswer(invocation -> {
            ProductEntity entity = invocation.getArgument(0);
            entity.setId(UUID.randomUUID());
            return entity;
        }).when(productRepository).save(any(ProductEntity.class));

        // Act
        UUID productId = productService.createProduct(creationData);

        // Assert
        assertThat(productId).isNotNull();
        verify(productRepository, times(1)).save(any(ProductEntity.class));
    }

    @Test
    void patchProduct_ProductNotFound_ThrowsNotFoundException() {
        // Arrange
        PatchProductRequest patchingData =
                new PatchProductRequest(UUID.randomUUID(), 99, new BigDecimal("99.99"));

        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(ProductNotFoundException.class,
                () -> productService.patchProduct(patchingData));

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, never()).save(any(ProductEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodPatchProduct")
    void patchProduct_TotalQuantityExceedsLimit_ThrowsProductOverflowException(PatchProductRequest patchingData,
                                                                               ProductEntity product) {
        // Arrange
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));

        // Act & Assert
        assertThrows(ProductOverflowException.class,
                () -> productService.patchProduct(patchingData));

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, never()).save(any(ProductEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodPatchProduct")
    void patchProduct_RepositorySaveFails_PropagatesException(PatchProductRequest patchingData,
                                                              ProductEntity product) {
        // Arrange
        product.setQuantity(1);

        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> productService.patchProduct(patchingData));

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
    }

    private static Stream<Arguments> createArgForMethodPatchProduct() {
        return Stream.of(
                Arguments.of(
                        new PatchProductRequest(UUID.randomUUID(), 99, new BigDecimal("99.99")),
                        ProductEntity.builder()
                                .id(UUID.randomUUID())
                                .name("seedlings")
                                .quantity(12)
                                .price(new BigDecimal("199.99"))
                                .build()
                )
        );
    }

    @ParameterizedTest @MethodSource("patchProduct_ValidPartialOrFullData")
    void patchProduct_ValidPartialOrFullData_UpdatesAndReturnsCorrectData(PatchProductRequest patchingData) {
        // Arrange
        int productQuantity = 12;
        ProductEntity product = ProductEntity.builder()
                .id(UUID.randomUUID())
                .name("seedlings")
                .quantity(productQuantity)
                .price(new BigDecimal("199.99"))
                .build();

        int patchingQuantity = patchingData.quantity() == null ? 0 : patchingData.quantity();
        BigDecimal patchingPrice = patchingData.price() == null ? product.getPrice() : patchingData.price();

        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenReturn(product);

        // Act
        ProductData data = productService.patchProduct(patchingData);

        // Assert
        assertThat(data).isNotNull();
        assertThat(data.id()).isEqualTo(product.getId());
        assertThat(data.name()).isEqualTo(product.getName());
        assertThat(data.quantity()).isEqualTo(productQuantity + patchingQuantity);
        assertThat(data.price()).isEqualTo(patchingPrice);

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
    }

    private static Stream<Arguments> patchProduct_ValidPartialOrFullData() {
        return Stream.of(
                Arguments.of(new PatchProductRequest(UUID.randomUUID(), 50, new BigDecimal("99.99"))),
                Arguments.of(new PatchProductRequest(UUID.randomUUID(), null, new BigDecimal("99.99"))),
                Arguments.of(new PatchProductRequest(UUID.randomUUID(), 50, null)),
                Arguments.of(new PatchProductRequest(UUID.randomUUID(), null, null))
        );
    }
}
