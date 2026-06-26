package by.pressf.productms.dao.repository;

import by.pressf.productms.dao.entity.ProductEntity;
import org.jspecify.annotations.NullMarked;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@NullMarked @Repository
public interface ProductRepository extends JpaRepository<ProductEntity, UUID> {
}
