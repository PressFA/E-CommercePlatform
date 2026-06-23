package by.pressf.paymentms.unit.dao.repository;

import by.pressf.paymentms.dao.entity.PaymentEntity;
import by.pressf.paymentms.dao.entity.type.PaymentType;
import by.pressf.paymentms.dao.repository.PaymentRepository;
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
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest(properties = {
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.show-sql=true",
        "spring.jpa.properties.hibernate.format_sql=true",
        "spring.liquibase.enabled=false"
})
class PaymentRepositoryUnitTests {
    @Autowired
    private PaymentRepository paymentRepository;
    @Autowired
    private TestEntityManager entityManager;

    @BeforeEach
    public void init() {
        paymentRepository.deleteAll();
    }

    @ParameterizedTest @MethodSource("findByOrderId_PaymentExists")
    void findByOrderId_PaymentExists_ReturnsPayment(List<PaymentEntity> payments) {
        // Arrange
        UUID orderId = payments.getLast().getOrderId();
        paymentRepository.saveAllAndFlush(payments);

        entityManager.clear();

        // Act
        PaymentEntity payment = paymentRepository.findByOrderId(orderId);

        // Assert
        assertThat(payment).isNotNull();
    }

    private static Stream<Arguments> findByOrderId_PaymentExists() {
        return Stream.of(
                Arguments.of(List.of(
                  new PaymentEntity(null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                          new BigDecimal("99.99"), LocalDateTime.now(), PaymentType.PAYMENT)
                )),
                Arguments.of(List.of(
                  new PaymentEntity(null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                          new BigDecimal("99.99"), LocalDateTime.now(), PaymentType.PAYMENT),
                  new PaymentEntity(null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                          new BigDecimal("89.99"), LocalDateTime.now(), PaymentType.TOP_UP),
                  new PaymentEntity(null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                          new BigDecimal("79.99"), LocalDateTime.now(), PaymentType.REFUND)
                ))
        );
    }

    @ParameterizedTest @MethodSource("findByOrderId_PaymentNotFound")
    void findByOrderId_PaymentNotFound_ReturnNull(List<PaymentEntity> payments) {
        // Arrange
        UUID orderId = UUID.randomUUID();
        paymentRepository.saveAllAndFlush(payments);

        entityManager.clear();

        // Act
        PaymentEntity payment = paymentRepository.findByOrderId(orderId);

        // Assert
        assertThat(payment).isNull();
    }

    private static Stream<Arguments> findByOrderId_PaymentNotFound() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(
                        new PaymentEntity(
                                null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                                new BigDecimal("99.99"), LocalDateTime.now(), PaymentType.PAYMENT
                        ),
                        new PaymentEntity(
                                null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                                new BigDecimal("89.99"), LocalDateTime.now(), PaymentType.TOP_UP
                        ),
                        new PaymentEntity(
                                null, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                                new BigDecimal("79.99"), LocalDateTime.now(), PaymentType.REFUND
                        )
                ))
        );
    }

    @Test
    void save_ValidPayment_ReturnSavedPayment() {
        // Arrange
        PaymentEntity payment = PaymentEntity.builder()
                .userId(UUID.randomUUID())
                .orderId(null)
                .stripeId(UUID.randomUUID().toString())
                .amount(new BigDecimal("99.99"))
                .type(PaymentType.PAYMENT)
                .build();

        // Act
        paymentRepository.saveAndFlush(payment);

        entityManager.clear();

        // Assert
        PaymentEntity savedPayment = paymentRepository.findById(payment.getId()).orElse(null);

        assertThat(savedPayment).isNotNull();
        assertThat(savedPayment.getUserId()).isEqualTo(payment.getUserId());
        assertThat(savedPayment.getOrderId()).isEqualTo(payment.getOrderId());
        assertThat(savedPayment.getStripeId()).isEqualTo(payment.getStripeId());
        assertThat(savedPayment.getAmount()).isEqualTo(payment.getAmount());
        assertThat(savedPayment.getCreatedAt()).isEqualTo(payment.getCreatedAt());
        assertThat(savedPayment.getType()).isEqualTo(payment.getType());
    }

    @Test
    void save_InvalidLength_ThrowsException() {
        // Arrange
        PaymentEntity payment = PaymentEntity.builder()
                .userId(UUID.randomUUID())
                .orderId(null)
                .stripeId("12345678901234567890123456789012345678901234567890")
                .amount(new BigDecimal("99.99"))
                .type(PaymentType.PAYMENT)
                .build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> paymentRepository.saveAndFlush(payment));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    @ParameterizedTest @MethodSource("save_NullArgument")
    void save_NullArgument_ThrowsException(PaymentEntity entity) {
        // Arrange
        PaymentEntity payment = PaymentEntity.builder()
                .userId(entity.getUserId())
                .orderId(entity.getOrderId())
                .stripeId(entity.getStripeId())
                .amount(entity.getAmount())
                .type(entity.getType())
                .build();

        // Act & Assert
        Throwable ex = assertThrows(DataAccessException.class, () -> paymentRepository.saveAndFlush(payment));

        assertThat(ex).isInstanceOf(DataIntegrityViolationException.class);
    }

    private static Stream<Arguments> save_NullArgument() {
        return Stream.of(
                Arguments.of(new PaymentEntity(null, null, null, UUID.randomUUID().toString(),
                        new BigDecimal("99.99"), null, PaymentType.PAYMENT)),
                Arguments.of(new PaymentEntity(null, UUID.randomUUID(), null, null,
                        new BigDecimal("99.99"), null, PaymentType.PAYMENT)),
                Arguments.of(new PaymentEntity(null, UUID.randomUUID(), null,
                        UUID.randomUUID().toString(), null, null, PaymentType.PAYMENT)),
                Arguments.of(new PaymentEntity(null, UUID.randomUUID(), null,
                        UUID.randomUUID().toString(), new BigDecimal("99.99"), null, null))
        );
    }

    @Test
    void update_UpdatableFields_StayUnchanged() {
        // Arrange
        PaymentEntity payment = PaymentEntity.builder()
                .userId(UUID.randomUUID())
                .orderId(null)
                .stripeId(UUID.randomUUID().toString())
                .amount(new BigDecimal("99.99"))
                .type(PaymentType.PAYMENT)
                .build();
        paymentRepository.saveAndFlush(payment);

        entityManager.clear();

        // Act
        PaymentEntity savedPayment = paymentRepository.findById(payment.getId()).orElse(null);
        assertThat(savedPayment).isNotNull();

        savedPayment.setUserId(null);
        savedPayment.setOrderId(UUID.randomUUID());
        savedPayment.setStripeId(null);
        savedPayment.setAmount(null);
        savedPayment.setCreatedAt(null);
        savedPayment.setType(null);
        paymentRepository.saveAndFlush(savedPayment);

        entityManager.clear();

        // Assert
        PaymentEntity changedPayment = paymentRepository.findById(payment.getId()).orElse(null);

        assertThat(changedPayment).isNotNull();
        assertThat(changedPayment.getUserId()).isEqualTo(payment.getUserId());
        assertThat(changedPayment.getOrderId()).isEqualTo(payment.getOrderId());
        assertThat(changedPayment.getStripeId()).isEqualTo(payment.getStripeId());
        assertThat(changedPayment.getAmount()).isEqualTo(payment.getAmount());
        assertThat(changedPayment.getCreatedAt()).isEqualTo(payment.getCreatedAt());
        assertThat(changedPayment.getType()).isEqualTo(payment.getType());
    }
}
