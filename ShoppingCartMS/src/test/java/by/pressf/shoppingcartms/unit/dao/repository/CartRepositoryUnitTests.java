package by.pressf.shoppingcartms.unit.dao.repository;

import by.pressf.shoppingcartms.dao.entity.CartEntity;
import by.pressf.shoppingcartms.dao.repository.ShoppingCartRepository;
import by.pressf.shoppingcartms.dto.CartInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
public class CartRepositoryUnitTests {
    @Autowired
    private ShoppingCartRepository cartRepository;
    @Autowired
    private TestEntityManager entityManager;

    @BeforeEach
    public void init() {
        cartRepository.deleteAll();
    }

    @ParameterizedTest @MethodSource("findAllByUserId_UserHasNoItems")
    void findAllByUserId_UserHasNoItems_ReturnEmptyList(List<CartEntity> entities) {
        // Arrange
        cartRepository.saveAllAndFlush(entities);
        UUID userId = UUID.randomUUID();

        entityManager.clear();

        // Act
        List<CartInfo> list = cartRepository.findAllByUserId(userId);

        // Assert
        assertThat(list).isEmpty();
    }

    private static Stream<Arguments> findAllByUserId_UserHasNoItems() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 1),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 2),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 3)
                ))
        );
    }

    @ParameterizedTest @MethodSource("findAllByUserId_UserHasItems")
    void findAllByUserId_UserHasItems_ReturnCartInfoList(List<CartEntity> entities) {
        // Arrange
        cartRepository.saveAllAndFlush(entities);
        UUID userId = UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e");

        entityManager.clear();

        // Act
        List<CartInfo> list = cartRepository.findAllByUserId(userId);

        // Assert
        assertThat(list)
                .isNotEmpty()
                .hasSize(2);
    }

    private static Stream<Arguments> findAllByUserId_UserHasItems() {
        return Stream.of(
                Arguments.of(List.of(
                        new CartEntity(null, UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e"), UUID.randomUUID(), 1),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 2),
                        new CartEntity(null, UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e"), UUID.randomUUID(), 3),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 4),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 5)
                )),
                Arguments.of(List.of(
                        new CartEntity(null, UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e"), UUID.randomUUID(), 1),
                        new CartEntity(null, UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e"), UUID.randomUUID(), 2)
                ))
        );
    }

    @ParameterizedTest @MethodSource("findAllByUserId_NullArgument")
    void findAllByUserId_NullArgument_ReturnEmptyList(List<CartEntity> entities) {
        // Arrange
        cartRepository.saveAllAndFlush(entities);

        entityManager.clear();

        // Act
        List<CartInfo> list = cartRepository.findAllByUserId(null);

        // Assert
        assertThat(list).isEmpty();
    }

    private static Stream<Arguments> findAllByUserId_NullArgument() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 1),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 2),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 3)
                ))
        );
    }

    @ParameterizedTest @MethodSource("findByUserIdAndProductId_CartEntityExists")
    void findByUserIdAndProductId_CartEntityExists_ReturnCartEntity(List<CartEntity> entities) {
        // Arrange
        UUID userId = entities.getLast().getUserId();
        UUID productId = entities.getLast().getProductId();
        cartRepository.saveAllAndFlush(entities);

        entityManager.clear();

        // Act
        CartEntity cartEntity = cartRepository.findByUserIdAndProductId(userId, productId);

        // Assert
        assertThat(cartEntity).isNotNull();
    }

    private static Stream<Arguments> findByUserIdAndProductId_CartEntityExists() {
        return Stream.of(
                Arguments.of(List.of(
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 1),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 2),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 3)
                ))
        );
    }

    @ParameterizedTest @MethodSource("findByUserIdAndProductId_CartEntityNotFound")
    void findByUserIdAndProductId_CartEntityNotFound_ReturnEmpty(UUID userId, UUID productId) {
        // Arrange
        cartRepository.saveAllAndFlush(List.of(
                new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 1),
                new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 2),
                new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 3)
        ));

        entityManager.clear();

        // Act
        CartEntity entity = cartRepository.findByUserIdAndProductId(userId, productId);

        // Assert
        assertThat(entity).isNull();
    }

    private static Stream<Arguments> findByUserIdAndProductId_CartEntityNotFound() {
        return Stream.of(
                Arguments.of(UUID.randomUUID(), UUID.randomUUID()),
                Arguments.of(null, UUID.randomUUID()),
                Arguments.of(UUID.randomUUID(), null),
                Arguments.of(null, null)
        );
    }

    @Test
    void save_ValidCartEntity_ReturnsSavedEntity() {
        // Arrange
        CartEntity entity = CartEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .build();

        // Act
        cartRepository.saveAndFlush(entity);

        entityManager.clear();

        // Assert
        CartEntity savedEntity = cartRepository.findById(entity.getId()).orElse(null);

        assertThat(savedEntity).isNotNull();
        assertThat(savedEntity.getUserId()).isEqualTo(entity.getUserId());
        assertThat(savedEntity.getProductId()).isEqualTo(entity.getProductId());
        assertThat(savedEntity.getQuantity()).isEqualTo(entity.getQuantity());
    }

    @ParameterizedTest @MethodSource("save_InvalidCartEntity")
    void save_InvalidCartEntity_ThrowsException(CartEntity entity) {
        // Arrange & Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> cartRepository.saveAndFlush(entity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_InvalidCartEntity() {
        return Stream.of(
                Arguments.of(new CartEntity(null, null, UUID.randomUUID(), 1)),
                Arguments.of(new CartEntity(null, UUID.randomUUID(), null, 1)),
                Arguments.of(new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), null))
        );
    }

    @ParameterizedTest @ValueSource(ints = {-1, 0, 1})
    void update_Quantity_ReturnUpdatedCartEntity(Integer quantity) {
        // Arrange
        CartEntity entity = CartEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .build();
        cartRepository.saveAndFlush(entity);

        entityManager.clear();

        // Act
        CartEntity savedEntity = cartRepository.findById(entity.getId()).orElse(null);
        assertThat(savedEntity).isNotNull();

        savedEntity.setQuantity(savedEntity.getQuantity() + quantity);

        cartRepository.saveAndFlush(savedEntity);

        entityManager.clear();

        // Assert
        CartEntity checkedEntity = cartRepository.findById(savedEntity.getId()).orElse(null);

        assertThat(checkedEntity).isNotNull();
        assertThat(checkedEntity.getQuantity()).isEqualTo(entity.getQuantity() + quantity);
    }

    @ParameterizedTest @MethodSource("update_UserIdAndProductId")
    void update_UserIdAndProductId_ReturnFirstEntity(UUID userId, UUID productId) {
        // Arrange
        CartEntity entity = CartEntity.builder()
                .userId(UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e"))
                .productId(UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e"))
                .quantity(5)
                .build();
        cartRepository.saveAndFlush(entity);

        entityManager.clear();

        // Act
        CartEntity savedEntity = cartRepository.findById(entity.getId()).orElse(null);
        assertThat(savedEntity).isNotNull();

        if (!userId.equals(savedEntity.getUserId())) {
            savedEntity.setUserId(userId);
        }
        if (!productId.equals(savedEntity.getProductId())) {
            savedEntity.setProductId(productId);
        }
        cartRepository.saveAndFlush(savedEntity);

        entityManager.clear();

        // Assert
        CartEntity checkedEntity = cartRepository.findById(savedEntity.getId()).orElse(null);

        assertThat(checkedEntity).isNotNull();
        assertThat(checkedEntity.getUserId()).isEqualTo(entity.getUserId());
        assertThat(checkedEntity.getProductId()).isEqualTo(entity.getProductId());
        assertThat(checkedEntity.getQuantity()).isEqualTo(entity.getQuantity());
    }

    private static Stream<Arguments> update_UserIdAndProductId() {
        return Stream.of(
                Arguments.of(UUID.randomUUID(), UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e")),
                Arguments.of(UUID.fromString("e30f63e1-09e6-4599-aada-fe2af26db92e"), UUID.randomUUID())
        );
    }

    @Test
    void update_NullArgument_ThrowsException() {
        // Arrange
        CartEntity entity = CartEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .build();
        cartRepository.saveAndFlush(entity);

        entityManager.clear();

        // Act & Assert
        CartEntity savedEntity = cartRepository.findById(entity.getId()).orElse(null);
        assertThat(savedEntity).isNotNull();

        Throwable ex = assertThrows(DataAccessException.class, () -> {
            savedEntity.setUserId(null);
            savedEntity.setProductId(null);
            savedEntity.setQuantity(null);
            cartRepository.saveAndFlush(savedEntity);
        });

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
        assertThat(ex.getMessage()).contains("NULL not allowed for column \"QUANTITY\"");
    }

    @ParameterizedTest @MethodSource("delete_CartEntityExists")
    void delete_CartEntityExists_Success(List<CartEntity> entities) {
        // Arrange
        cartRepository.saveAllAndFlush(entities);
        UUID id = entities.getLast().getId();

        entityManager.clear();

        // Act
        CartEntity entity = cartRepository.findById(id).orElse(null);
        assertThat(entity).isNotNull();

        cartRepository.delete(entity);
        cartRepository.flush();

        entityManager.clear();

        // Assert
        List<CartEntity> entityList = cartRepository.findAll();

        assertThat(entityList)
                .isNotNull()
                .hasSize(entities.size() - 1);
    }

    private static Stream<Arguments> delete_CartEntityExists() {
        return Stream.of(
                Arguments.of(List.of(
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 4),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 5)
                )),
                Arguments.of(List.of(
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 1),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 2),
                        new CartEntity(null, UUID.randomUUID(), UUID.randomUUID(), 3)
                ))
        );
    }
}
