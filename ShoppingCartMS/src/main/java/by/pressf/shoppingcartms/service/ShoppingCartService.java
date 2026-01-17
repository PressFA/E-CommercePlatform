package by.pressf.shoppingcartms.service;

import by.pressf.core.exceptions.AppError;
import by.pressf.shoppingcartms.dao.entity.CartEntity;
import by.pressf.shoppingcartms.dao.repository.ShoppingCartRepository;
import by.pressf.shoppingcartms.dto.CartInfo;
import by.pressf.shoppingcartms.dto.CreateCartRequest;
import by.pressf.shoppingcartms.dto.QuantityChangeCart;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ShoppingCartService {
    private final ShoppingCartRepository shoppingCartRepository;

    @Transactional(readOnly = true)
    public List<CartInfo> getShoppingCartsByUser(UUID userId) {
        return shoppingCartRepository.findAllByUserId(userId);
    }

    @Transactional
    public CartInfo createCart(CreateCartRequest cartRequest) {
        CartEntity cart = CartEntity.builder()
                .userId(cartRequest.userId())
                .productId(cartRequest.productId())
                .quantity(cartRequest.quantity())
                .build();

        shoppingCartRepository.save(cart);

        return new CartInfo(cart.getId(), cart.getUserId(),
                cart.getProductId(), cart.getQuantity());
    }

    @Transactional
    public CartInfo updateQuantity(QuantityChangeCart changeCart) {
        CartEntity cart = shoppingCartRepository.findById(changeCart.id())
                .orElseThrow(() -> new AppError(404,
                        "Cart item with id " + changeCart.id() + " not found"));

        if (cart.getQuantity() + changeCart.quantity() <= 0) {
            throw new AppError(400, "Quantity cannot be less than 1. Use DELETE to remove item.");
        }

        cart.setQuantity(cart.getQuantity() + changeCart.quantity());

        shoppingCartRepository.save(cart);

        return new CartInfo(cart.getId(), cart.getUserId(),
                cart.getProductId(), cart.getQuantity());
    }

    @Transactional
    public void removeItemFromCart(UUID id) {
        CartEntity cart = shoppingCartRepository.findById(id)
                .orElseThrow(() -> new AppError(404,
                        "Cart item with id " + id + " not found"));

        shoppingCartRepository.delete(cart);
    }
}
