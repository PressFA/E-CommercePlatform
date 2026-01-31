package by.pressf.orderms.dao.entity;

import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data @Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity @Table(name = "orders_history")
public class OrderHistoryEntity {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Column(name = "order_id")
    private UUID orderId;
    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private OrderHistoryStatus status;
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    @Column(columnDefinition = "TEXT")
    private String reason;
}
