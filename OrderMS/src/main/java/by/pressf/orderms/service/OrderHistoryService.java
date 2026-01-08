package by.pressf.orderms.service;

import by.pressf.orderms.dao.entity.OrderHistoryEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.OrderHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderHistoryService {
    private final OrderHistoryRepository orderHistoryRepository;

    public void createOrderHistory(UUID orderId) {
        OrderHistoryEntity entity = OrderHistoryEntity.builder()
                .orderId(orderId)
                .createdAt(LocalDateTime.now())
                .status(OrderStatus.CREATED)
                .build();
        orderHistoryRepository.save(entity);
    }

    public void approveOrderHistory(UUID orderId) {
        OrderHistoryEntity entity = orderHistoryRepository.findByOrderId(orderId);

        entity.setStatus(OrderStatus.APPROVED);
        entity.setUpdatedAt(LocalDateTime.now());

        orderHistoryRepository.save(entity);
    }

    public void rejectOrderHistory(UUID orderId) {
        OrderHistoryEntity entity = orderHistoryRepository.findByOrderId(orderId);

        entity.setStatus(OrderStatus.REJECTED);
        entity.setUpdatedAt(LocalDateTime.now());

        orderHistoryRepository.save(entity);
    }
}
