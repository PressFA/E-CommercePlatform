package by.pressf.userms.dao.repository;

import by.pressf.userms.dao.entity.EventEntity;
import org.jspecify.annotations.NonNull;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EventRepository extends JpaRepository<EventEntity, Long> {
    EventEntity findByMessageId(@NonNull String messageId);
}
