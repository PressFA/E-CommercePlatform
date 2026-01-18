package by.pressf.shoppingcartms.service;

import by.pressf.core.dto.events.CreateOrderShoppingCart;
import by.pressf.core.exceptions.AppError;
import by.pressf.shoppingcartms.dao.entity.CartEntity;
import by.pressf.shoppingcartms.dao.repository.ShoppingCartRepository;
import by.pressf.shoppingcartms.dto.CartInfo;
import by.pressf.shoppingcartms.dto.CreateCartRequest;
import by.pressf.shoppingcartms.dto.QuantityChangeCart;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartService {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ShoppingCartRepository shoppingCartRepository;

    @Transactional(value = "transactionManager", readOnly = true)
    public List<CartInfo> getShoppingCartsByUser(UUID userId) {
        return shoppingCartRepository.findAllByUserId(userId);
    }

    @Transactional("transactionManager")
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
    public void createOrderFromShoppingCart(UUID id) {
        CartEntity cart = shoppingCartRepository.findById(id)
                .orElseThrow(() -> new AppError(404,
                        "Cart item with id " + id + " not found"));

        CreateOrderShoppingCart event = new CreateOrderShoppingCart(
                cart.getUserId(),
                cart.getProductId(),
                cart.getQuantity()
        );

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(
                        env.getRequiredProperty("shopping-cart.events.topic.name"),
                        cart.getId().toString(),
                        event
                );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The CreateOrderShoppingCart message was sent to the cart-checkout-initiated topic.");

        shoppingCartRepository.delete(cart);
        log.info("Shopping cart item id: {} has been successfully deleted from database after order placement", id);
    }
}
