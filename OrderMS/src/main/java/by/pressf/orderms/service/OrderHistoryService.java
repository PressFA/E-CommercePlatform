package by.pressf.orderms.service;

import by.pressf.orderms.dao.entity.OrderHistoryEntity;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.dao.repository.OrderHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderHistoryService {
    private final OrderHistoryRepository orderHistoryRepository;

    public void createSuccessLog(UUID orderId, String reason) {
        OrderHistoryEntity entity = OrderHistoryEntity.builder()
                .orderId(orderId)
                .status(OrderHistoryStatus.SUCCESS)
                .createdAt(LocalDateTime.now())
                .reason(reason)
                .build();

        orderHistoryRepository.save(entity);
    }

    public void createFailLog(UUID orderId, String reason) {
        OrderHistoryEntity entity = OrderHistoryEntity.builder()
                .orderId(orderId)
                .status(OrderHistoryStatus.FAIL)
                .createdAt(LocalDateTime.now())
                .reason(reason)
                .build();

        orderHistoryRepository.save(entity);
    }
}
