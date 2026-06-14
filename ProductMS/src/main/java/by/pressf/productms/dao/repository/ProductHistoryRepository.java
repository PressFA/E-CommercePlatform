package by.pressf.productms.dao.repository;

import by.pressf.productms.dao.entity.ProductHistoryEntity;
import org.jspecify.annotations.NullMarked;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@NullMarked @Repository
public interface ProductHistoryRepository extends JpaRepository<ProductHistoryEntity, UUID> {
    ProductHistoryEntity findByOrderId(UUID orderId);
}
