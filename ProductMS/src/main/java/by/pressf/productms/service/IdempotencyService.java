package by.pressf.productms.service;

import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.productms.dao.entity.EventEntity;
import by.pressf.productms.dao.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {
    private final EventRepository eventRepository;

    public void idempotenceCheck(String messageId, String nameClass) {
        EventEntity processedEvent = eventRepository.findByMessageId(messageId);
        if (processedEvent != null) {
            throw new DuplicateMessageException(messageId);
        }
        log.info("The message {} passed the idempotence check", nameClass);
    }

    public void saveIdempotentKey(String messageId, String nameClass) {
        eventRepository.save(EventEntity.builder()
                .messageId(messageId)
                .build());
        log.info("The {} message with messageId={} has been processed", nameClass, messageId);
    }
}
