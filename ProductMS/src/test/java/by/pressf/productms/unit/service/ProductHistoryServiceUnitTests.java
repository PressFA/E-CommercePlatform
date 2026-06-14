package by.pressf.productms.unit.service;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.entity.ProductHistoryEntity;
import by.pressf.productms.dao.entity.status.ProductStatus;
import by.pressf.productms.dao.repository.ProductHistoryRepository;
import by.pressf.productms.dao.repository.ProductRepository;
import by.pressf.productms.dto.internal.ProductReservationRequest;
import by.pressf.productms.exception.ProductHistoryNotFoundException;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundByOrderIdException;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.service.ProductHistoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
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
public class ProductHistoryServiceUnitTests {
    private @Mock ProductRepository productRepository;
    private @Mock ProductHistoryRepository productHistoryRepository;
    private @InjectMocks ProductHistoryService productHistoryService;

    @ParameterizedTest @NullSource
    void reserveProduct_RequestIsNull_ThrowsNpe(ProductReservationRequest req) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> productHistoryService.reserveProduct(req));

        verify(productRepository, never()).findById(any(UUID.class));
        verify(productRepository, never()).save(any(ProductEntity.class));
        verify(productHistoryRepository, never()).save(any(ProductHistoryEntity.class));
    }

    @Test
    void reserveProduct_ProductNotFound_ThrowsNotFoundException() {
        // Arrange
        ProductReservationRequest req =
                new ProductReservationRequest(UUID.randomUUID(), UUID.randomUUID(), 10);

        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(ProductNotFoundException.class,
                () -> productHistoryService.reserveProduct(req));

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, never()).save(any(ProductEntity.class));
        verify(productHistoryRepository, never()).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodReserveProduct")
    void reserveProduct_InsufficientQuantity_ThrowsInsufficientException(ProductReservationRequest req,
                                                                         ProductEntity product) {
        // Arrange
        product.setQuantity(3);
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));

        // Act & Assert
        assertThrows(ProductInsufficientException.class,
                () -> productHistoryService.reserveProduct(req));

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, never()).save(any(ProductEntity.class));
        verify(productHistoryRepository, never()).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodReserveProduct")
    void reserveProduct_ProductRepositorySaveFails_ThrowsDataAccessException(ProductReservationRequest req,
                                                                             ProductEntity product) {
        // Arrange
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenThrow(new DataIntegrityViolationException(null));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> productHistoryService.reserveProduct(req));

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
        verify(productHistoryRepository, never()).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodReserveProduct")
    void reserveProduct_HistoryRepositorySaveFails_ThrowsDataAccessException(ProductReservationRequest req,
                                                                             ProductEntity product) {
        // Arrange
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenReturn(product);
        when(productHistoryRepository.save(any(ProductHistoryEntity.class)))
                .thenThrow(new DataIntegrityViolationException(null));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> productHistoryService.reserveProduct(req));

        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
        verify(productHistoryRepository, times(1)).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodReserveProduct")
    void reserveProduct_ValidRequest_ReturnsTotalCostAndSavesData(ProductReservationRequest req,
                                                                  ProductEntity product) {
        // Arrange
        ProductHistoryEntity history = ProductHistoryEntity.builder()
                .orderId(req.orderId())
                .product(product)
                .quantity(req.quantity())
                .status(ProductStatus.RESERVED)
                .build();

        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenReturn(product);
        when(productHistoryRepository.save(any(ProductHistoryEntity.class)))
                .thenReturn(history);

        // Act
        BigDecimal totalCostProduct = productHistoryService.reserveProduct(req);

        // Assert
        assertThat(totalCostProduct).isNotNull()
                        .isEqualByComparingTo(product.getPrice().multiply(BigDecimal.valueOf(req.quantity())));
        assertThat(product.getQuantity()).isEqualTo(req.quantity());
        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
        verify(productHistoryRepository, times(1)).save(any(ProductHistoryEntity.class));
    }

    private static Stream<Arguments> createArgForMethodReserveProduct() {
        return Stream.of(
                Arguments.of(
                        new ProductReservationRequest(UUID.randomUUID(), UUID.randomUUID(), 5),
                        ProductEntity.builder()
                                .name("seedlings")
                                .quantity(10)
                                .price(new BigDecimal("100.00"))
                                .build()
                )
        );
    }

    @ParameterizedTest @MethodSource("createArgForMethodCancelProductReservation")
    void cancelProductReservation_HistoryNotFound_ThrowsNotFoundException(ProductEntity product,
                                                                          ProductHistoryEntity history) {
        // Arrange
        history.setProduct(product);

        when(productHistoryRepository.findByOrderId(any(UUID.class)))
                .thenReturn(null);

        // Act & Assert
        assertThrows(ProductHistoryNotFoundException.class,
                () -> productHistoryService.cancelProductReservation(UUID.randomUUID()));

        verify(productHistoryRepository, times(1)).findByOrderId(any(UUID.class));
        verify(productRepository, never()).findById(any(UUID.class));
        verify(productRepository, never()).save(any(ProductEntity.class));
        verify(productHistoryRepository, never()).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodCancelProductReservation")
    void cancelProductReservation_ProductInHistoryNotFound_ThrowsNotFoundException(ProductEntity product,
                                                                                   ProductHistoryEntity history) {
        // Arrange
        history.setProduct(product);

        when(productHistoryRepository.findByOrderId(any(UUID.class)))
                .thenReturn(history);
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(ProductNotFoundByOrderIdException.class,
                () -> productHistoryService.cancelProductReservation(UUID.randomUUID()));

        verify(productHistoryRepository, times(1)).findByOrderId(any(UUID.class));
        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, never()).save(any(ProductEntity.class));
        verify(productHistoryRepository, never()).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodCancelProductReservation")
    void cancelProductReservation_ProductRepositorySaveFails_PropagatesException(ProductEntity product,
                                                                                 ProductHistoryEntity history) {
        // Arrange
        history.setProduct(product);

        when(productHistoryRepository.findByOrderId(any(UUID.class)))
                .thenReturn(history);
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> productHistoryService.cancelProductReservation(UUID.randomUUID()));

        verify(productHistoryRepository, times(1)).findByOrderId(any(UUID.class));
        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
        verify(productHistoryRepository, never()).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodCancelProductReservation")
    void cancelProductReservation_HistoryRepositorySaveFails_PropagatesException(ProductEntity product,
                                                                                 ProductHistoryEntity history) {
        // Arrange
        history.setProduct(product);

        when(productHistoryRepository.findByOrderId(any(UUID.class)))
                .thenReturn(history);
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenReturn(product);
        when(productHistoryRepository.save(any(ProductHistoryEntity.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> productHistoryService.cancelProductReservation(UUID.randomUUID()));

        verify(productHistoryRepository, times(1)).findByOrderId(any(UUID.class));
        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
        verify(productHistoryRepository, times(1)).save(any(ProductHistoryEntity.class));
    }

    @ParameterizedTest @MethodSource("createArgForMethodCancelProductReservation")
    void cancelProductReservation_ValidOrderId_RestoresQuantityAndSetsCancelledStatus(ProductEntity product,
                                                                                      ProductHistoryEntity history) {
        // Arrange
        int mustBe = product.getQuantity() + history.getQuantity();
        history.setProduct(product);

        when(productHistoryRepository.findByOrderId(any(UUID.class)))
                .thenReturn(history);
        when(productRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(product));
        when(productRepository.save(any(ProductEntity.class)))
                .thenReturn(product);
        when(productHistoryRepository.save(any(ProductHistoryEntity.class)))
                .thenReturn(history);

        // Act
        productHistoryService.cancelProductReservation(UUID.randomUUID());

        // Assert
        assertThat(product.getQuantity()).isEqualTo(mustBe);
        assertThat(history.getStatus()).isEqualTo(ProductStatus.CANCELLED);
        verify(productHistoryRepository, times(1)).findByOrderId(any(UUID.class));
        verify(productRepository, times(1)).findById(any(UUID.class));
        verify(productRepository, times(1)).save(any(ProductEntity.class));
        verify(productHistoryRepository, times(1)).save(any(ProductHistoryEntity.class));
    }

    private static Stream<Arguments> createArgForMethodCancelProductReservation() {
        return Stream.of(
                Arguments.of(
                        ProductEntity.builder().id(UUID.randomUUID()).name("seedlings").quantity(4)
                                .price(new BigDecimal("100.00")).build(),
                        ProductHistoryEntity.builder()
                                .id(UUID.randomUUID())
                                .orderId(UUID.randomUUID())
                                .quantity(5)
                                .status(ProductStatus.RESERVED)
                                .build()
                )
        );
    }
}
