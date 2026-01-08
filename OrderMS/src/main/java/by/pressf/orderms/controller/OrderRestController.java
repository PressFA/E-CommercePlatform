package by.pressf.orderms.controller;

import by.pressf.orderms.dto.CreateOrderRequest;
import by.pressf.orderms.dto.OrderCreationData;
import by.pressf.orderms.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/order")
public class OrderRestController {
    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest req) {
        log.info("I received a request from a user with the ID {} to create an order for an item with the ID {}, quantity {}",
                req.userId(), req.productId(), req.quantity());

        OrderCreationData orderCreationData = new OrderCreationData(
                req.userId(),
                req.productId(),
                req.quantity()
        );

        UUID orderId = orderService.createOrder(orderCreationData);

        return ResponseEntity.status(HttpStatus.CREATED).body(orderId);
    }
}
