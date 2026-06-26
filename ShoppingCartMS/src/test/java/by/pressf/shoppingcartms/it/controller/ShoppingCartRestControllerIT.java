package by.pressf.shoppingcartms.it.controller;

import by.pressf.core.dto.orchestration.events.cart.CreateOrderShoppingCart;
import by.pressf.shoppingcartms.dao.entity.CartEntity;
import by.pressf.shoppingcartms.dto.incoming.BuyProductRequest;
import by.pressf.shoppingcartms.dto.incoming.CreateCartRequest;
import by.pressf.shoppingcartms.dto.incoming.QuantityChangeCart;
import by.pressf.shoppingcartms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class ShoppingCartRestControllerIT extends BaseIT {
    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        shoppingCartRepository.deleteAll();
    }

    @Test
    void should_ReturnCartList_When_UserHasItemsInCart() throws Exception {
        // Arrange
        UUID userId = UUID.randomUUID();
        CartEntity cart = CartEntity.builder()
                .userId(userId)
                .productId(UUID.randomUUID())
                .quantity(2)
                .build();
        shoppingCartRepository.save(cart);

        // Act & Assert
        mockMvc.perform(get("/api/v1/cart/{userId}", userId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].userId").value(userId.toString()))
                .andExpect(jsonPath("$[0].quantity").value(2));
    }

    @Test
    void should_ReturnBadRequest_When_UserIdHasInvalidIdFormat() throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(get("/api/v1/cart/invalid-uuid-string"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Invalid parameter type"));
    }

    @Test
    void should_CreateCartItemSuccessfully_When_RequestIsValid() throws Exception {
        // Arrange
        CreateCartRequest request = new CreateCartRequest(UUID.randomUUID(), UUID.randomUUID(), 3);

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.quantity").value(3));

        List<CartEntity> allCarts = shoppingCartRepository.findAll();
        assertThat(allCarts).hasSize(1);
        assertThat(allCarts.getFirst().getQuantity()).isEqualTo(3);
    }

    @Test
    void should_ReturnBadRequest_When_ProductAlreadyInCart() throws Exception {
        // Arrange
        UUID userId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        CartEntity existingCart = CartEntity.builder()
                .userId(userId)
                .productId(productId)
                .quantity(1)
                .build();
        shoppingCartRepository.save(existingCart);

        CreateCartRequest request = new CreateCartRequest(userId, productId, 2);

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message")
                        .value("This product is already in the shopping cart. You can't add the same product twice."));
    }

    @Test
    void should_PlaceOrderAndPublishToKafkaAndReturnNoContent_When_CartItemExists() throws Exception {
        // Arrange
        CartEntity cart = CartEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .build();
        shoppingCartRepository.save(cart);

        BuyProductRequest request = new BuyProductRequest(cart.getId(), "developer@pressf.by");

        // Act
        mockMvc.perform(post("/api/v1/cart/buy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isNoContent());

        assertThat(shoppingCartRepository.findById(cart.getId())).isEmpty();

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(5));

        assertThat(record.key()).isEqualTo(cart.getId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        CreateOrderShoppingCart event = mapper.readValue(record.value(), CreateOrderShoppingCart.class);
        assertThat(event.userId()).isEqualTo(cart.getUserId());
        assertThat(event.username()).isEqualTo("developer@pressf.by");
        assertThat(event.productId()).isEqualTo(cart.getProductId());
        assertThat(event.quantity()).isEqualTo(5);
    }

    @Test
    void should_ReturnNotFound_When_CartItemDoesNotExistOnCheckout() throws Exception {
        // Arrange
        BuyProductRequest request = new BuyProductRequest(UUID.randomUUID(), "developer@pressf.by");

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart/buy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message")
                        .value("Cart item with id " + request.id() + " not found"));

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(3)));
    }

    @Test
    void should_UpdateQuantitySuccessfully_When_NewQuantityIsValid() throws Exception {
        // Arrange
        CartEntity cart = CartEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(3)
                .build();
        shoppingCartRepository.save(cart);

        QuantityChangeCart request = new QuantityChangeCart(cart.getId(), 1);

        // Act & Assert
        mockMvc.perform(patch("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.quantity").value(4));

        CartEntity updatedCart = shoppingCartRepository.findById(cart.getId()).orElseThrow();
        assertThat(updatedCart.getQuantity()).isEqualTo(4);
    }

    @Test
    void should_ReturnBadRequest_When_QuantityBecomesLessThanOne() throws Exception {
        // Arrange
        CartEntity cart = CartEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(1)
                .build();
        shoppingCartRepository.save(cart);

        QuantityChangeCart request = new QuantityChangeCart(cart.getId(), -1);

        // Act & Assert
        mockMvc.perform(patch("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message")
                        .value("Quantity cannot be less than 1. Use DELETE to remove item."));
    }

    @Test
    void should_RemoveItemFromCart_When_CartItemExists() throws Exception {
        // Arrange
        CartEntity cart = CartEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(2)
                .build();
        shoppingCartRepository.save(cart);

        // Act & Assert
        mockMvc.perform(delete("/api/v1/cart/{id}", cart.getId()))
                .andExpect(status().isNoContent());

        assertThat(shoppingCartRepository.findById(cart.getId())).isEmpty();
    }

    @Test
    void should_ReturnNotFound_When_CartItemDoesNotExistOnDelete() throws Exception {
        // Arrange
        UUID nonExistentId = UUID.randomUUID();

        // Act & Assert
        mockMvc.perform(delete("/api/v1/cart/{id}", nonExistentId))

                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message")
                        .value("Cart item with id " + nonExistentId + " not found"));
    }
}
