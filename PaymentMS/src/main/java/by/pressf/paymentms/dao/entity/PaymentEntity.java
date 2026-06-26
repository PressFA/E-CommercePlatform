package by.pressf.paymentms.dao.entity;

import by.pressf.paymentms.dao.entity.type.PaymentType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Data @Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity @Table(name = "payments")
public class PaymentEntity {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Column(name = "user_id", nullable = false, updatable = false)
    private UUID userId;
    @Column(name = "order_id", updatable = false)
    private UUID orderId;
    @Column(name = "stripe_id", nullable = false, updatable = false, length = 48)
    private String stripeId;
    @Column(nullable = false, updatable = false)
    private BigDecimal amount;
    @Builder.Default
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
    @Column(nullable = false, updatable = false, length = 12)
    @Enumerated(EnumType.STRING)
    private PaymentType type;
}
