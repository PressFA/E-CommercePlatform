package by.pressf.shoppingcartms.dao.repository;

import by.pressf.shoppingcartms.dao.entity.CartEntity;
import by.pressf.shoppingcartms.dto.internal.CartInfo;
import org.jspecify.annotations.NonNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ShoppingCartRepository extends JpaRepository<CartEntity, UUID> {
    List<CartInfo> findAllByUserId(@NonNull UUID userId);

    CartEntity findByUserIdAndProductId(@NonNull UUID userId, @NonNull UUID productId);
}
