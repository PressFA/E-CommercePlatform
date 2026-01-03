package by.pressf.orderms.dao.entity;

import by.pressf.orderms.dao.entity.status.OrderStatus;
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
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "order_id")
    private UUID orderId;
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private OrderStatus status;
}
