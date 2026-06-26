package by.pressf.shoppingcartms.unit.controller;

import by.pressf.core.exceptions.AppError;
import by.pressf.shoppingcartms.controller.ShoppingCartRestController;
import by.pressf.shoppingcartms.dto.incoming.BuyProductRequest;
import by.pressf.shoppingcartms.dto.incoming.CreateCartRequest;
import by.pressf.shoppingcartms.dto.incoming.QuantityChangeCart;
import by.pressf.shoppingcartms.dto.internal.CartInfo;
import by.pressf.shoppingcartms.service.ShoppingCartService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.kafka.KafkaException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ShoppingCartRestController.class)
public class ShoppingCartRestControllerUnitTests {
    @MockitoBean
    private ShoppingCartService shoppingCartService;
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper mapper;

    @Test
    void getShoppingCartsByUserId_InvalidPathVariable_ReturnsBadRequest() throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(get("/api/v1/cart/{userId}", "not-a-uuid-format"))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Invalid parameter type")
                );

        verifyNoInteractions(shoppingCartService);
    }

    @Test
    void getShoppingCartsByUserId_ServiceThrowsDataAccessException_ReturnsInternalServerError() throws Exception {
        // Arrange
        when(shoppingCartService.getShoppingCartsByUser(any(UUID.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        mockMvc.perform(get("/api/v1/cart/{userId}", UUID.randomUUID()))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(shoppingCartService, times(1)).getShoppingCartsByUser(any(UUID.class));
    }

    @Test
    void getShoppingCartsByUserId_NoCartsFound_ReturnsEmptyList() throws Exception {
        // Arrange
        when(shoppingCartService.getShoppingCartsByUser(any(UUID.class))).thenReturn(Collections.emptyList());

        // Act & Assert
        mockMvc.perform(get("/api/v1/cart/{userId}", UUID.randomUUID()))
                .andExpectAll(
                        status().isOk(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verify(shoppingCartService, times(1)).getShoppingCartsByUser(any(UUID.class));
    }

    @Test
    void getShoppingCartsByUserId_CartsFound_ReturnsCartList() throws Exception {
        // Arrange
        UUID userId = UUID.randomUUID();
        CartInfo cart1 = new CartInfo(UUID.randomUUID(), userId, UUID.randomUUID(), 1);
        CartInfo cart2 = new CartInfo(UUID.randomUUID(), userId, UUID.randomUUID(), 2);
        CartInfo cart3 = new CartInfo(UUID.randomUUID(), userId, UUID.randomUUID(), 3);

        when(shoppingCartService.getShoppingCartsByUser(any(UUID.class)))
                .thenReturn(List.of(cart1, cart2, cart3));

        // Act & Assert
        mockMvc.perform(get("/api/v1/cart/{userId}", userId))
                .andExpectAll(
                        status().isOk(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.length()").value(3),
                        jsonPath("$[0].id").value(cart1.id().toString()),
                        jsonPath("$[0].userId").value(userId.toString()),
                        jsonPath("$[1].productId").value(cart2.productId().toString()),
                        jsonPath("$[2].quantity").value(3)
                );

        verify(shoppingCartService, times(1)).getShoppingCartsByUser(any(UUID.class));
    }

    @ParameterizedTest @MethodSource("addToCart")
    void addToCart_InvalidData_ReturnsBadRequest(CreateCartRequest invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(post("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(shoppingCartService);
    }

    private static Stream<Arguments> addToCart() {
        UUID userId = UUID.randomUUID(), productId = UUID.randomUUID();

        return Stream.of(
                Arguments.of(new CreateCartRequest(null, productId, 5)),
                Arguments.of(new CreateCartRequest(userId, null, 5)),
                Arguments.of(new CreateCartRequest(userId, productId, null)),
                Arguments.of(new CreateCartRequest(userId, productId, 0)),
                Arguments.of(new CreateCartRequest(userId, productId, 11)),
                Arguments.of(new CreateCartRequest(null, null, 0))
        );
    }

    @Test
    void addToCart_ProductAlreadyExists_ReturnsBadRequest() throws Exception {
        // Arrange
        CreateCartRequest request = new CreateCartRequest(UUID.randomUUID(), UUID.randomUUID(), 1);

        when(shoppingCartService.createCart(any(CreateCartRequest.class)))
                .thenThrow(new AppError(400, "This product is already in the shopping cart. You can't add the same product twice."));

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.message").value("This product is already in the shopping cart. You can't add the same product twice.")
                );

        verify(shoppingCartService, times(1)).createCart(any(CreateCartRequest.class));
    }

    @Test
    void addToCart_DatabaseDown_ReturnsInternalServerError() throws Exception {
        // Arrange
        CreateCartRequest request = new CreateCartRequest(UUID.randomUUID(), UUID.randomUUID(), 1);

        when(shoppingCartService.createCart(any(CreateCartRequest.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(shoppingCartService, times(1)).createCart(any(CreateCartRequest.class));
    }

    @Test
    void addToCart_ValidData_ReturnsCreated() throws Exception {
        // Arrange
        UUID cartId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        CartInfo response = new CartInfo(cartId, userId, productId, 1);
        CreateCartRequest request = new CreateCartRequest(userId, productId, 1);

        when(shoppingCartService.createCart(any(CreateCartRequest.class))).thenReturn(response);

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isCreated(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.id").value(cartId.toString()),
                        jsonPath("$.userId").value(userId.toString()),
                        jsonPath("$.productId").value(productId.toString()),
                        jsonPath("$.quantity").value(1)
                );

        verify(shoppingCartService, times(1)).createCart(any(CreateCartRequest.class));
    }

    @ParameterizedTest @MethodSource("provideInvalidBuyRequests")
    void placeOrder_InvalidData_ReturnsBadRequest(BuyProductRequest invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(post("/api/v1/cart/buy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(shoppingCartService);
    }

    private static Stream<Arguments> provideInvalidBuyRequests() {
        UUID id = UUID.randomUUID();
        return Stream.of(
                Arguments.of(new BuyProductRequest(null, "valid@test.com")),
                Arguments.of(new BuyProductRequest(id, null)),
                Arguments.of(new BuyProductRequest(id, "")),
                Arguments.of(new BuyProductRequest(id, "not-an-email"))
        );
    }

    @Test
    void placeOrder_AppError404_ReturnsNotFound() throws Exception {
        // Arrange
        BuyProductRequest request = new BuyProductRequest(UUID.randomUUID(), "test@test.com");

        doThrow(new AppError(404, "Cart item not found"))
                .when(shoppingCartService).createOrderFromShoppingCart(any(BuyProductRequest.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart/buy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isNotFound(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.message").value("Cart item not found")
                );

        verify(shoppingCartService, times(1))
                .createOrderFromShoppingCart(any(BuyProductRequest.class));
    }

    @Test
    void placeOrder_KafkaException_ReturnsInternalServerError() throws Exception {
        // Arrange
        BuyProductRequest request = new BuyProductRequest(UUID.randomUUID(), "test@test.com");

        doThrow(mock(KafkaException.class))
                .when(shoppingCartService).createOrderFromShoppingCart(any(BuyProductRequest.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart/buy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("An unexpected error occurred")
                );

        verify(shoppingCartService, times(1))
                .createOrderFromShoppingCart(any(BuyProductRequest.class));
    }

    @Test
    void placeOrder_DatabaseError_ReturnsInternalServerError() throws Exception {
        // Arrange
        BuyProductRequest request = new BuyProductRequest(UUID.randomUUID(), "test@test.com");

        doThrow(mock(DataAccessException.class))
                .when(shoppingCartService).createOrderFromShoppingCart(any(BuyProductRequest.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart/buy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(shoppingCartService, times(1))
                .createOrderFromShoppingCart(any(BuyProductRequest.class));
    }

    @Test
    void placeOrder_ValidData_ReturnsNoContent() throws Exception {
        // Arrange
        BuyProductRequest request = new BuyProductRequest(UUID.randomUUID(), "test@test.com");

        doNothing().when(shoppingCartService).createOrderFromShoppingCart(any(BuyProductRequest.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/cart/buy")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isNoContent());

        verify(shoppingCartService, times(1))
                .createOrderFromShoppingCart(any(BuyProductRequest.class));
    }

    @ParameterizedTest @MethodSource("provideInvalidUpdateQuantity")
    void updateQuantity_InvalidData_ReturnsBadRequest(QuantityChangeCart invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(patch("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(shoppingCartService);
    }

    private static Stream<Arguments> provideInvalidUpdateQuantity() {
        UUID id = UUID.randomUUID();
        return Stream.of(
                Arguments.of(new QuantityChangeCart(null, 1)),
                Arguments.of(new QuantityChangeCart(id, null)),
                Arguments.of(new QuantityChangeCart(id, -2)),
                Arguments.of(new QuantityChangeCart(id, 2))
        );
    }

    @Test
    void updateQuantity_AppError404_ReturnsNotFound() throws Exception {
        // Arrange
        QuantityChangeCart request = new QuantityChangeCart(UUID.randomUUID(), 1);

        when(shoppingCartService.updateQuantity(any(QuantityChangeCart.class)))
                .thenThrow(new AppError(404, "Cart item not found"));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isNotFound(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.message").value("Cart item not found")
                );

        verify(shoppingCartService, times(1)).updateQuantity(any(QuantityChangeCart.class));
    }

    @Test
    void updateQuantity_AppError400_ReturnsBadRequest() throws Exception {
        // Arrange
        QuantityChangeCart request = new QuantityChangeCart(UUID.randomUUID(), 1);

        when(shoppingCartService.updateQuantity(any(QuantityChangeCart.class)))
                .thenThrow(new AppError(400, "Invalid operation"));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.message").value("Invalid operation")
                );

        verify(shoppingCartService, times(1)).updateQuantity(any(QuantityChangeCart.class));
    }

    @Test
    void updateQuantity_DatabaseError_ReturnsInternalServerError() throws Exception {
        // Arrange
        QuantityChangeCart request = new QuantityChangeCart(UUID.randomUUID(), 1);

        when(shoppingCartService.updateQuantity(any(QuantityChangeCart.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(shoppingCartService, times(1)).updateQuantity(any(QuantityChangeCart.class));
    }

    @Test
    void updateQuantity_ValidData_ReturnsUpdatedCart() throws Exception {
        // Arrange
        UUID cartId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        QuantityChangeCart request = new QuantityChangeCart(cartId, 1);
        CartInfo response = new CartInfo(cartId, userId, productId, 2);

        when(shoppingCartService.updateQuantity(any(QuantityChangeCart.class)))
                .thenReturn(response);

        // Act & Assert
        mockMvc.perform(patch("/api/v1/cart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isOk(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.id").value(cartId.toString()),
                        jsonPath("$.quantity").value(2)
                );

        verify(shoppingCartService, times(1)).updateQuantity(any(QuantityChangeCart.class));
    }

    @Test
    void removeItem_InvalidPathVariable_ReturnsBadRequest() throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(delete("/api/v1/cart/{id}", "not-a-uuid-format"))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Invalid parameter type")
                );

        verifyNoInteractions(shoppingCartService);
    }

    @Test
    void removeItem_AppError404_ReturnsNotFound() throws Exception {
        // Arrange
        doThrow(new AppError(404, "Cart item not found"))
                .when(shoppingCartService).removeItemFromCart(any(UUID.class));

        // Act & Assert
        mockMvc.perform(delete("/api/v1/cart/{id}", UUID.randomUUID()))
                .andExpectAll(
                        status().isNotFound(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.message").value("Cart item not found")
                );

        verify(shoppingCartService, times(1)).removeItemFromCart(any(UUID.class));
    }

    @Test
    void removeItem_DatabaseError_ReturnsInternalServerError() throws Exception {
        // Arrange
        doThrow(mock(DataAccessException.class))
                .when(shoppingCartService).removeItemFromCart(any(UUID.class));

        // Act & Assert
        mockMvc.perform(delete("/api/v1/cart/{id}", UUID.randomUUID()))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(shoppingCartService, times(1)).removeItemFromCart(any(UUID.class));
    }

    @Test
    void removeItem_ValidId_ReturnsNoContent() throws Exception {
        // Arrange
        doNothing().when(shoppingCartService).removeItemFromCart(any(UUID.class));

        // Act & Assert
        mockMvc.perform(delete("/api/v1/cart/{id}", UUID.randomUUID()))
                .andExpect(status().isNoContent());

        verify(shoppingCartService, times(1)).removeItemFromCart(any(UUID.class));
    }
}
