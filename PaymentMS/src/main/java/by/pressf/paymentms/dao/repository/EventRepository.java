package by.pressf.paymentms.dao.repository;

import by.pressf.paymentms.dao.entity.EventEntity;
import org.jspecify.annotations.NonNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EventRepository extends JpaRepository<EventEntity, Long> {
    EventEntity findByMessageId(@NonNull String messageId);
}
