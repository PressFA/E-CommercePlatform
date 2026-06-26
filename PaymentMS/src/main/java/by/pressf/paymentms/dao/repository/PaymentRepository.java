package by.pressf.paymentms.dao.repository;

import by.pressf.paymentms.dao.entity.PaymentEntity;
import org.jspecify.annotations.NullMarked;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@NullMarked @Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity, UUID> {
    PaymentEntity findByOrderId(UUID orderId);
}
