package by.pressf.orderms.dao.repository;

import by.pressf.orderms.dao.entity.EventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EventRepository extends JpaRepository<EventEntity, Long> {
    EventEntity findByMessageId(String messageId);
}
