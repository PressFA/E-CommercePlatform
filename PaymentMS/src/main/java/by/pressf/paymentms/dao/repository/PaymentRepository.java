package by.pressf.paymentms.dao.repository;

import by.pressf.paymentms.dao.entity.PaymentEntity;
import org.jspecify.annotations.NonNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity, UUID> {
    PaymentEntity findByOrderId(@NonNull UUID orderId);
}
