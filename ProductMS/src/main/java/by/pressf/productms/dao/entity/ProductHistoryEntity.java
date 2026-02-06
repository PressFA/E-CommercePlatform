package by.pressf.productms.dao.entity;

import by.pressf.productms.dao.entity.status.ProductStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Data @Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity @Table(name = "product_history")
public class ProductHistoryEntity {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Column(name = "order_id", nullable = false, updatable = false)
    private UUID orderId;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "product_id", nullable = false, updatable = false)
    private ProductEntity product;
    @Column(nullable = false, updatable = false)
    private Integer quantity;
    @Builder.Default
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    @Column(nullable = false, length = 12)
    @Enumerated(EnumType.STRING)
    private ProductStatus status;
}
