package by.pressf.productms.unit.dao.repository;

import by.pressf.productms.dao.entity.EventEntity;
import by.pressf.productms.dao.repository.EventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
class EventRepositoryUnitTests {
    @Autowired
    private EventRepository eventRepository;
    @Autowired
    private TestEntityManager entityManager;

    @BeforeEach
    public void init() {
        eventRepository.deleteAll();
    }

    @Test
    void findByMessageId_MessageExists_ReturnProcessedMessage() {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        eventRepository.saveAllAndFlush(List.of(
                EventEntity.builder().messageId(UUID.randomUUID().toString()).build(),
                EventEntity.builder().messageId(messageId).build(),
                EventEntity.builder().messageId(UUID.randomUUID().toString()).build()
        ));

        entityManager.clear();

        // Act
        EventEntity entity = eventRepository.findByMessageId(messageId);

        // Assert
        assertThat(entity).isNotNull();
        assertThat(entity.getMessageId()).isEqualTo(messageId);
    }

    @ParameterizedTest @MethodSource("findByMessageId_MessageNotFound")
    void findByMessageId_MessageNotFound_ReturnNull(List<EventEntity> entities) {
        // Arrange
        eventRepository.saveAllAndFlush(entities);

        entityManager.clear();

        // Act
        EventEntity entity = eventRepository.findByMessageId(UUID.randomUUID().toString());

        // Assert
        assertThat(entity).isNull();
    }

    private static Stream<Arguments> findByMessageId_MessageNotFound() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(
                        EventEntity.builder().messageId(UUID.randomUUID().toString()).build(),
                        EventEntity.builder().messageId(UUID.randomUUID().toString()).build(),
                        EventEntity.builder().messageId(UUID.randomUUID().toString()).build()
                ))
        );
    }

    @ParameterizedTest @MethodSource("findByMessageId_NullArgument")
    void findByMessageId_NullArgument_ReturnNull(List<EventEntity> entities) {
        // Arrange
        eventRepository.saveAllAndFlush(entities);

        entityManager.clear();

        // Act
        EventEntity entity = eventRepository.findByMessageId(null);

        // Assert
        assertThat(entity).isNull();
    }

    private static Stream<Arguments> findByMessageId_NullArgument() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(
                        EventEntity.builder().messageId(UUID.randomUUID().toString()).build(),
                        EventEntity.builder().messageId(UUID.randomUUID().toString()).build(),
                        EventEntity.builder().messageId(UUID.randomUUID().toString()).build()
                ))
        );
    }

    @Test
    void save_ValidEventEntity_ReturnSavedEntity() {
        // Arrange
        EventEntity entity = EventEntity.builder().messageId(UUID.randomUUID().toString()).build();

        // Act
        eventRepository.saveAndFlush(entity);

        entityManager.clear();

        // Assert
        EventEntity savedEntity = eventRepository.findById(entity.getId()).orElse(null);

        assertThat(savedEntity).isNotNull();
        assertThat(savedEntity.getMessageId()).isEqualTo(entity.getMessageId());
    }

    @Test
    void save_MessageIdToNull_ThrowsException() {
        // Arrange
        EventEntity entity = EventEntity.builder().messageId(null).build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> eventRepository.saveAndFlush(entity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void save_DuplicateMessageId_ThrowsException() {
        // Arrange
        UUID id = UUID.randomUUID();
        EventEntity entity1 = EventEntity.builder().messageId(id.toString()).build(),
                entity2 = EventEntity.builder().messageId(id.toString()).build();
        eventRepository.saveAndFlush(entity1);

        entityManager.clear();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> eventRepository.saveAndFlush(entity2));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void save_InvalidLength_ThrowsException() {
        // Arrange
        EventEntity entity = EventEntity.builder()
                .messageId("12345678901234567890123456789012345678901234567890")
                .build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> eventRepository.saveAndFlush(entity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    @ParameterizedTest @MethodSource("update_MessageId")
    void update_MessageId_StayUnchanged(String messageId) {
        // Arrange
        EventEntity entity = EventEntity.builder().messageId(UUID.randomUUID().toString()).build();
        eventRepository.saveAndFlush(entity);

        entityManager.clear();

        // Act
        EventEntity savedEntity = eventRepository.findById(entity.getId()).orElse(null);
        assertThat(savedEntity).isNotNull();

        savedEntity.setMessageId(messageId);
        eventRepository.saveAndFlush(savedEntity);

        entityManager.clear();

        // Assert
        EventEntity changedEntity = eventRepository.findById(entity.getId()).orElse(null);

        assertThat(changedEntity).isNotNull();
        assertThat(changedEntity.getMessageId()).isEqualTo(entity.getMessageId());
    }

    private static Stream<Arguments> update_MessageId() {
        return Stream.of(
                Arguments.of(UUID.randomUUID().toString()),
                Arguments.of((Object) null)
        );
    }
}
