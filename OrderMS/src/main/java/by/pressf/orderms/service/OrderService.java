package by.pressf.orderms.service;

import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.OrderRepository;
import by.pressf.orderms.exception.OrderNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;

    public void approveOrder(UUID orderId) {
        OrderEntity entity = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        entity.setStatus(OrderStatus.APPROVED);

        orderRepository.save(entity);
    }
}
