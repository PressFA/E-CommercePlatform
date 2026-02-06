package by.pressf.userms.unit.dao.repository;

import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dao.repository.UserRepository;
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

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
class UserRepositoryUnitTests {
    @Autowired
    private TestEntityManager entityManager;
    @Autowired
    private UserRepository userRepository;

    @BeforeEach
    public void init() {
        userRepository.deleteAll();
    }

    @Test
    void save_ValidUser_ReturnSavedUser() {
        // Arrange
        UserEntity user = UserEntity.builder()
                .username("test@mail.com")
                .password("password")
                .name("Danny")
                .build();

        // Act
        userRepository.saveAndFlush(user);

        entityManager.clear();

        // Assert
        UserEntity savedUser = userRepository.findById(user.getId()).orElse(null);

        assertThat(savedUser).isNotNull();
        assertThat(savedUser.getUsername()).isEqualTo(user.getUsername());
        assertThat(savedUser.getPassword()).isEqualTo(user.getPassword());
        assertThat(savedUser.getName()).isEqualTo(user.getName());
        assertThat(savedUser.getBalance()).isEqualByComparingTo(user.getBalance());
        assertThat(savedUser.getVersion()).isEqualTo(user.getVersion());
    }

    @ParameterizedTest @MethodSource("save_InvalidUserEntity")
    void save_InvalidUserEntity_ThrowsException(UserEntity entity) {
        // Arrange & Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> userRepository.saveAndFlush(entity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_InvalidUserEntity() {
        return Stream.of(
                Arguments.of(new UserEntity(null, null, "password", "name", null, null)),
                Arguments.of(new UserEntity(null, "username", null, "name", null, null)),
                Arguments.of(new UserEntity(null, "username", "password", null, null, null))
        );
    }

    @Test
    void save_DuplicateUsername_ThrowsException() {
        // Arrange
        UserEntity user1 = UserEntity.builder()
                .username("test@mail.com")
                .password("password")
                .name("Danny")
                .build(),
        user2 = UserEntity.builder()
                .username("test@mail.com")
                .password("password")
                .name("Danny")
                .build();
        userRepository.saveAndFlush(user1);

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> userRepository.saveAndFlush(user2));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    @ParameterizedTest @MethodSource("save_InvalidLength")
    void save_InvalidLength_ThrowsException(UserEntity entity) {
        // Arrange & Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> userRepository.saveAndFlush(entity));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_InvalidLength() {
        return Stream.of(
                Arguments.of(new UserEntity(null, "123456789012345678901234567890", "password", "name", BigDecimal.ZERO, 0)),
                Arguments.of(new UserEntity(null, "username", "123456789012345678901234567890", "name", BigDecimal.ZERO, 0)),
                Arguments.of(new UserEntity(null, "username", "password", "123456789012345678901234567890", BigDecimal.ZERO, 0))
        );
    }

    @ParameterizedTest @MethodSource("update_Balance")
    void update_Balance_Success(BigDecimal amount, BigDecimal finalBalance) {
        // Arrange
        UserEntity user = UserEntity.builder()
                .username("test@mail.com")
                .password("password")
                .name("Danny")
                .balance(new BigDecimal("9999.99"))
                .build();
        userRepository.saveAndFlush(user);

        entityManager.clear();

        // Act
        UserEntity savedUser = userRepository.findById(user.getId()).orElse(null);
        assertThat(savedUser).isNotNull();

        savedUser.setBalance(savedUser.getBalance().add(amount));
        userRepository.saveAndFlush(savedUser);

        entityManager.clear();

        // Assert
        UserEntity changedUser = userRepository.findById(user.getId()).orElse(null);

        assertThat(changedUser).isNotNull();
        assertThat(changedUser.getBalance()).isEqualByComparingTo(finalBalance);
        assertThat(changedUser.getVersion()).isNotEqualTo(user.getVersion());
    }

    private static Stream<Arguments> update_Balance() {
        return Stream.of(
                Arguments.of(new BigDecimal("-99.99"), new BigDecimal("9900")),
                Arguments.of(new BigDecimal("99.01"), new BigDecimal("10099"))
        );
    }

    @Test
    void update_BalanceToNull_ThrowsException() {
        // Arrange
        UserEntity entity = UserEntity.builder()
                .username("test@mail.com")
                .password("password")
                .name("Danny")
                .build();
        userRepository.saveAndFlush(entity);

        entityManager.clear();

        // Act & Assert
        UserEntity savedUser = userRepository.findById(entity.getId()).orElse(null);
        assertThat(savedUser).isNotNull();

        Throwable ex = assertThrows(DataAccessException.class, () -> {
            savedUser.setBalance(null);
            userRepository.saveAndFlush(savedUser);
        });

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }
}
