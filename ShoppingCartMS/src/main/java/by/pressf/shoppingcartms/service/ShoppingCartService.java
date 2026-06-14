package by.pressf.shoppingcartms.service;

import by.pressf.core.dto.orchestration.events.cart.CreateOrderShoppingCart;
import by.pressf.core.exceptions.AppError;
import by.pressf.shoppingcartms.dao.entity.CartEntity;
import by.pressf.shoppingcartms.dao.repository.ShoppingCartRepository;
import by.pressf.shoppingcartms.dto.incoming.BuyProductRequest;
import by.pressf.shoppingcartms.dto.internal.CartInfo;
import by.pressf.shoppingcartms.dto.incoming.CreateCartRequest;
import by.pressf.shoppingcartms.dto.incoming.QuantityChangeCart;
import by.pressf.shoppingcartms.kafka.publisher.KafkaEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@NullMarked
@RequiredArgsConstructor
public class ShoppingCartService {
    private final KafkaEventPublisher kafkaEventPublisher;
    private final ShoppingCartRepository shoppingCartRepository;

    @Transactional(value = "transactionManager", readOnly = true)
    public List<CartInfo> getShoppingCartsByUser(UUID userId) {
        return shoppingCartRepository.findAllByUserId(userId);
    }

    @Transactional("transactionManager")
    public CartInfo createCart(CreateCartRequest cartRequest) {
        CartEntity checkCart = shoppingCartRepository.findByUserIdAndProductId(cartRequest.userId(), cartRequest.productId());

        if (checkCart != null) {
            throw new AppError(400, "This product is already in the shopping cart. You can't add the same product twice.");
        }

        CartEntity cart = CartEntity.builder()
                .userId(cartRequest.userId())
                .productId(cartRequest.productId())
                .quantity(cartRequest.quantity())
                .build();

        shoppingCartRepository.save(cart);

        return new CartInfo(cart.getId(), cart.getUserId(),
                cart.getProductId(), cart.getQuantity());
    }

    @Transactional("transactionManager")
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

    @Transactional("transactionManager")
    public void removeItemFromCart(UUID id) {
        CartEntity cart = shoppingCartRepository.findById(id)
                .orElseThrow(() -> new AppError(404,
                        "Cart item with id " + id + " not found"));

        shoppingCartRepository.delete(cart);
    }

    @Transactional("transactionManager")
    public void createOrderFromShoppingCart(BuyProductRequest buyRequest) {
        CartEntity cart = shoppingCartRepository.findById(buyRequest.id())
                .orElseThrow(() -> new AppError(404,
                        "Cart item with id " + buyRequest.id() + " not found"));

        CreateOrderShoppingCart event = new CreateOrderShoppingCart(
                cart.getUserId(),
                buyRequest.username(),
                cart.getProductId(),
                cart.getQuantity()
        );

        kafkaEventPublisher.sendMessageCreateOrderShoppingCart(cart.getId().toString(), event);

        shoppingCartRepository.delete(cart);
        log.info("Shopping cart item id: {} has been successfully deleted from database after order placement", buyRequest.id());
    }
}
