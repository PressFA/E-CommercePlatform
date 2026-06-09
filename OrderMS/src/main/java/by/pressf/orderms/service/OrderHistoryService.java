package by.pressf.orderms.service;

import by.pressf.orderms.dao.entity.OrderHistoryEntity;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.dao.repository.OrderHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderHistoryService {
    private final OrderHistoryRepository orderHistoryRepository;

    public void createHistoryLog(@NonNull UUID orderId, @NonNull OrderHistoryStatus status, @NonNull String reason) {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(reason, "reason must not be null");

        OrderHistoryEntity entity = OrderHistoryEntity.builder()
                .orderId(orderId)
                .status(status)
                .reason(reason)
                .build();

        orderHistoryRepository.save(entity);
    }
}
