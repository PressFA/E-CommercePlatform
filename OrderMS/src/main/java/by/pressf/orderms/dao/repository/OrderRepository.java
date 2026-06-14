package by.pressf.orderms.dao.repository;

import by.pressf.orderms.dao.entity.OrderEntity;
import org.jspecify.annotations.NullMarked;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@NullMarked @Repository
public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
}
