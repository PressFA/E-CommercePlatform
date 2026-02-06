package by.pressf.shoppingcartms.dao.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data @Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity @Table(name = "shopping_carts")
public class CartEntity {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Column(name = "user_id", nullable = false, updatable = false)
    private UUID userId;
    @Column(name = "product_id", nullable = false, updatable = false)
    private UUID productId;
    @Column(nullable = false)
    private Integer quantity;
}
