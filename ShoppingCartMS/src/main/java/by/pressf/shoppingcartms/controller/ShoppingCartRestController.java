package by.pressf.shoppingcartms.controller;

import by.pressf.core.exceptions.AppError;
import by.pressf.shoppingcartms.dto.CartInfo;
import by.pressf.shoppingcartms.dto.CreateCartRequest;
import by.pressf.shoppingcartms.dto.QuantityChangeCart;
import by.pressf.shoppingcartms.service.ShoppingCartService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/cart")
public class ShoppingCartRestController {
    private final ShoppingCartService shoppingCartService;

    @GetMapping("/{userId}")
    public ResponseEntity<?> getShoppingCartsByUserId(@PathVariable UUID userId) {
        log.info("Received request to get shopping carts for userId: {}", userId);
        List<CartInfo> shoppingCarts = shoppingCartService.getShoppingCartsByUser(userId);

        log.info("Successfully retrieved {} items for userId: {}", shoppingCarts.size(), userId);
        return ResponseEntity.status(HttpStatus.OK).body(shoppingCarts);
    }

    @PostMapping
    public ResponseEntity<?> addToCart(@RequestBody @Valid CreateCartRequest cartRequest) {
        log.info("Received request to add item to cart for userId: {}, productId: {}",
                cartRequest.userId(), cartRequest.productId());
        CartInfo cartInfo = shoppingCartService.createCart(cartRequest);

        log.info("Successfully created cart item with id: {}", cartInfo.id());
        return ResponseEntity.status(HttpStatus.CREATED).body(cartInfo);
    }

    @PostMapping("/buy/{id}")
    public ResponseEntity<?> placeOrder(@PathVariable UUID id) {
        log.info("Received request to place order from shopping cart item id: {}", id);
        shoppingCartService.createOrderFromShoppingCart(id);

        log.info("Successfully processed checkout for cart item id: {}", id);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    @PatchMapping
    public ResponseEntity<?> updateQuantityItemCart(@RequestBody @Valid QuantityChangeCart changeCart) {
        log.info("Received request to update quantity for cart item id: {}. Change: {}",
                changeCart.id(), changeCart.quantity());
        CartInfo cartInfo = shoppingCartService.updateQuantity(changeCart);

        log.info("Successfully updated quantity for cart item id: {}. New quantity: {}",
                cartInfo.id(), cartInfo.quantity());
        return ResponseEntity.status(HttpStatus.OK).body(cartInfo);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> removeItem(@PathVariable UUID id) {
        log.info("Received request to remove cart item with id: {}", id);
        shoppingCartService.removeItemFromCart(id);

        log.info("Successfully removed cart item with id: {}", id);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
