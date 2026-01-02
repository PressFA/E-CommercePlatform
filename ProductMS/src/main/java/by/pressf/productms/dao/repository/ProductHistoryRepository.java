package by.pressf.productms.dao.repository;

import by.pressf.productms.dao.entity.ProductHistoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ProductHistoryRepository extends JpaRepository<ProductHistoryEntity, UUID> {
    ProductHistoryEntity findByOrderId(UUID orderId);
}
