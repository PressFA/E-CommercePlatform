package by.pressf.paymentms.dao.entity;

import by.pressf.paymentms.dao.entity.type.PaymentType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data @Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity @Table(name = "payments")
public class PaymentEntity {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Column(name = "user_id", nullable = false)
    private UUID userId;
    @Column(name = "order_id")
    private UUID orderId;
    @Column(nullable = false)
    private BigDecimal amount;
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private PaymentType type;
}
