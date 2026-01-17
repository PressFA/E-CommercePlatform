package by.pressf.shoppingcartms.controller;

import by.pressf.core.exceptions.AppError;
import by.pressf.shoppingcartms.dto.CartInfo;
import by.pressf.shoppingcartms.dto.CreateCartRequest;
import by.pressf.shoppingcartms.dto.QuantityChangeCart;
import by.pressf.shoppingcartms.service.ShoppingCartService;
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
        List<CartInfo> shoppingCarts = shoppingCartService.getShoppingCartsByUser(userId);

        return ResponseEntity.status(HttpStatus.OK).body(shoppingCarts);
    }

    @PostMapping
    public ResponseEntity<?> addToCart(@RequestBody CreateCartRequest cartRequest) {
        CartInfo cartInfo = shoppingCartService.createCart(cartRequest);

        return ResponseEntity.status(HttpStatus.CREATED).body(cartInfo);
    }

    @PatchMapping
    public ResponseEntity<?> updateQuantityItemCart(@RequestBody QuantityChangeCart changeCart) {
        try {
            CartInfo cartInfo = shoppingCartService.updateQuantity(changeCart);

            return ResponseEntity.status(HttpStatus.OK).body(cartInfo);
        } catch (AppError e) {
            log.error(e.getMessage());
            return ResponseEntity.status(e.getStatus()).body(e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> removeItem(@PathVariable UUID id) {
        try {
            shoppingCartService.removeItemFromCart(id);

            return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
        } catch (AppError e) {
            log.error(e.getMessage());
            return ResponseEntity.status(e.getStatus()).body(e.getMessage());
        }
    }
}
