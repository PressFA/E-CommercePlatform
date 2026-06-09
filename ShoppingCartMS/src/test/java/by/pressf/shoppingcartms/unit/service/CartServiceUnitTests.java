package by.pressf.shoppingcartms.unit.service;

import by.pressf.core.exceptions.AppError;
import by.pressf.shoppingcartms.dao.entity.CartEntity;
import by.pressf.shoppingcartms.dao.repository.ShoppingCartRepository;
import by.pressf.shoppingcartms.dto.incoming.CreateCartRequest;
import by.pressf.shoppingcartms.dto.incoming.QuantityChangeCart;
import by.pressf.shoppingcartms.dto.internal.CartInfo;
import by.pressf.shoppingcartms.service.ShoppingCartService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CartServiceUnitTests {
    @Mock
    private Environment env;
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Mock
    private ShoppingCartRepository shoppingCartRepository;

    @InjectMocks
    private ShoppingCartService cartService;

    @ParameterizedTest @NullSource
    void getShoppingCartsByUser_UserIdIsNull_ThrowsNpe(UUID userId) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> cartService.getShoppingCartsByUser(userId));

        verify(shoppingCartRepository, never()).findAllByUserId(any(UUID.class));
    }

    @Test
    void getShoppingCartsByUser_RepositoryFails_PropagatesDataAccessException() {
        // Arrange
        when(shoppingCartRepository.findAllByUserId(any(UUID.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> cartService.getShoppingCartsByUser(UUID.randomUUID()));

        verify(shoppingCartRepository, times(1)).findAllByUserId(any(UUID.class));
    }

    @ParameterizedTest @MethodSource("getShoppingCartsByUser")
    void getShoppingCartsByUser_ValidId_ReturnsList(List<CartInfo> carts) {
        // Arrange
        when(shoppingCartRepository.findAllByUserId(any(UUID.class)))
                .thenReturn(carts);

        // Act
        List<CartInfo> result = cartService.getShoppingCartsByUser(UUID.randomUUID());

        // Assert
        assertThat(result).isNotNull();

        verify(shoppingCartRepository, times(1)).findAllByUserId(any(UUID.class));
    }

    private static Stream<Arguments> getShoppingCartsByUser() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(mock(CartInfo.class), mock(CartInfo.class), mock(CartInfo.class)))
        );
    }

    @ParameterizedTest @NullSource
    void createCart_RequestIsNull_ThrowsNpe(CreateCartRequest cartRequest) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> cartService.createCart(cartRequest));

        verify(shoppingCartRepository, never()).findByUserIdAndProductId(any(UUID.class), any(UUID.class));
        verify(shoppingCartRepository, never()).save(any(CartEntity.class));
    }

    @ParameterizedTest @MethodSource("createCart")
    void createCart_ProductAlreadyInCart_ThrowsAppError(CreateCartRequest cartRequest) {
        // Arrange
        when(shoppingCartRepository.findByUserIdAndProductId(any(UUID.class), any(UUID.class)))
                .thenReturn(mock(CartEntity.class));

        // Act & Assert
        assertThrows(AppError.class,
                () -> cartService.createCart(cartRequest));

        verify(shoppingCartRepository, times(1)).findByUserIdAndProductId(any(UUID.class), any(UUID.class));
        verify(shoppingCartRepository, never()).save(any(CartEntity.class));
    }

    @ParameterizedTest @MethodSource("createCart")
    void createCart_RepositorySaveFails_PropagatesDataAccessException(CreateCartRequest cartRequest) {
        // Arrange
        when(shoppingCartRepository.findByUserIdAndProductId(any(UUID.class), any(UUID.class)))
                .thenReturn(null);
        when(shoppingCartRepository.save(any(CartEntity.class)))
                .thenThrow(mock(DataIntegrityViolationException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> cartService.createCart(cartRequest));

        verify(shoppingCartRepository, times(1)).findByUserIdAndProductId(any(UUID.class), any(UUID.class));
        verify(shoppingCartRepository, times(1)).save(any(CartEntity.class));
    }

    @ParameterizedTest @MethodSource("createCart")
    void createCart_ValidRequest_ReturnsCartInfo(CreateCartRequest cartRequest) {
        // Arrange
        when(shoppingCartRepository.findByUserIdAndProductId(any(UUID.class), any(UUID.class)))
                .thenReturn(null);
        doAnswer(invocation -> {
            CartEntity entity = invocation.getArgument(0);
            entity.setId(UUID.randomUUID());
            return entity;
        }).when(shoppingCartRepository).save(any(CartEntity.class));

        // Act
        CartInfo result = cartService.createCart(cartRequest);

        assertThat(result).isNotNull();
        assertThat(result.id()).isNotNull();
        assertThat(result.userId()).isEqualTo(cartRequest.userId());
        assertThat(result.productId()).isEqualTo(cartRequest.productId());
        assertThat(result.quantity()).isEqualTo(cartRequest.quantity());

        verify(shoppingCartRepository, times(1)).findByUserIdAndProductId(any(UUID.class), any(UUID.class));
        verify(shoppingCartRepository, times(1)).save(any(CartEntity.class));
    }

    private static Stream<Arguments> createCart() {
        return Stream.of(
                Arguments.of(new CreateCartRequest(UUID.randomUUID(), UUID.randomUUID(), 10))
        );
    }

    @ParameterizedTest @NullSource
    void updateQuantity_RequestIsNull_ThrowsNpe(QuantityChangeCart changeCart) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> cartService.updateQuantity(changeCart));

        verify(shoppingCartRepository, never()).findById(any(UUID.class));
        verify(shoppingCartRepository, never()).save(any(CartEntity.class));
    }

    @ParameterizedTest @MethodSource("updateQuantity")
    void updateQuantity_ItemNotFound_ThrowsAppError(QuantityChangeCart changeCart) {
        // Arrange
        when(shoppingCartRepository.findById(any(UUID.class)))
                .thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(AppError.class,
                () -> cartService.updateQuantity(changeCart));

        verify(shoppingCartRepository, times(1)).findById(any(UUID.class));
        verify(shoppingCartRepository, never()).save(any(CartEntity.class));
    }

    @ParameterizedTest @MethodSource("updateQuantity")
    void updateQuantity_ResultingQuantityIsZeroOrLess_ThrowsAppError(QuantityChangeCart changeCart) {
        // Arrange
        CartEntity mockEntity = mock(CartEntity.class);
        mockEntity.setQuantity(1);

        when(shoppingCartRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(mockEntity));

        // Act & Assert
        assertThrows(AppError.class,
                () -> cartService.updateQuantity(changeCart));

        verify(shoppingCartRepository, times(1)).findById(any(UUID.class));
        verify(shoppingCartRepository, never()).save(any(CartEntity.class));
    }

    @ParameterizedTest @MethodSource("updateQuantity")
    void updateQuantity_RepositorySaveFails_PropagatesDataAccessException(QuantityChangeCart changeCart) {
        // Arrange
        CartEntity cartEntity = CartEntity.builder()
                .id(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(2)
                .build();

        when(shoppingCartRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(cartEntity));
        when(shoppingCartRepository.save(any(CartEntity.class)))
                .thenThrow(mock(DataIntegrityViolationException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> cartService.updateQuantity(changeCart));

        verify(shoppingCartRepository, times(1)).findById(any(UUID.class));
        verify(shoppingCartRepository, times(1)).save(any(CartEntity.class));
    }

    @ParameterizedTest @MethodSource("updateQuantity")
    void updateQuantity_ValidRequest_UpdatesAndReturnsInfo(QuantityChangeCart changeCart) {
        // Arrange
        CartEntity cartEntity = CartEntity.builder()
                .id(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(2)
                .build();

        when(shoppingCartRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(cartEntity));
        when(shoppingCartRepository.save(any(CartEntity.class)))
                .thenReturn(cartEntity);

        // Act
        CartInfo result = cartService.updateQuantity(changeCart);

        assertThat(result).isNotNull();
        assertThat(result.id()).isEqualTo(cartEntity.getId());
        assertThat(result.userId()).isEqualTo(cartEntity.getUserId());
        assertThat(result.productId()).isEqualTo(cartEntity.getProductId());
        assertThat(result.quantity()).isEqualTo(cartEntity.getQuantity());
    }

    private static Stream<Arguments> updateQuantity() {
        return Stream.of(
                Arguments.of(new QuantityChangeCart(UUID.randomUUID(), -1))
        );
    }

    @ParameterizedTest @NullSource
    void removeItemFromCart_IdIsNull_ThrowsNpe(UUID cartId) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> cartService.removeItemFromCart(cartId));

        verify(shoppingCartRepository, never()).findById(any(UUID.class));
        verify(shoppingCartRepository, never()).delete(any(CartEntity.class));
    }

    @Test
    void removeItemFromCart_ItemNotFound_ThrowsAppError() {
        // Arrange
        when(shoppingCartRepository.findById(any(UUID.class)))
                .thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(AppError.class,
                () -> cartService.removeItemFromCart(UUID.randomUUID()));

        verify(shoppingCartRepository, times(1)).findById(any(UUID.class));
        verify(shoppingCartRepository, never()).delete(any(CartEntity.class));
    }

    @Test
    void removeItemFromCart_RepositoryDeleteFails_PropagatesDataAccessException() {
        // Arrange
        when(shoppingCartRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(mock(CartEntity.class)));
        doThrow(mock(DataAccessException.class)).when(shoppingCartRepository).delete(any(CartEntity.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> cartService.removeItemFromCart(UUID.randomUUID()));

        verify(shoppingCartRepository, times(1)).findById(any(UUID.class));
        verify(shoppingCartRepository, times(1)).delete(any(CartEntity.class));
    }

    @Test
    void removeItemFromCart_ValidId_DeletesItem() {
        // Arrange
        when(shoppingCartRepository.findById(any(UUID.class)))
                .thenReturn(Optional.of(mock(CartEntity.class)));

        // Act
        cartService.removeItemFromCart(UUID.randomUUID());

        // Assert
        verify(shoppingCartRepository, times(1)).findById(any(UUID.class));
        verify(shoppingCartRepository, times(1)).delete(any(CartEntity.class));
    }
}
