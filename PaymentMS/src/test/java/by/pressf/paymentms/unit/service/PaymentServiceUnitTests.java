package by.pressf.paymentms.unit.service;

import by.pressf.paymentms.dao.entity.PaymentEntity;
import by.pressf.paymentms.dao.repository.PaymentRepository;
import by.pressf.paymentms.dto.*;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.exception.PaymentNotFoundByOrderIdException;
import by.pressf.paymentms.service.PaymentService;
import by.pressf.paymentms.service.StripeService;
import com.stripe.exception.StripeException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaymentServiceUnitTests {
    private @Mock StripeService stripeService;
    private @Mock PaymentRepository paymentRepository;
    private @InjectMocks PaymentService paymentService;

    @ParameterizedTest @NullSource
    void createOrderPayment_RequestIsNull_ThrowsNpe(CreateOrderPaymentRequest req) throws StripeException {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> paymentService.createOrderPayment(req));

        verify(stripeService, never()).createPayment(any(StripeOrderPaymentDto.class));
        verify(paymentRepository, never()).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createCreateOrderPaymentRequest")
    void createOrderPayment_StripeServiceThrowsException_ThrowsPaymentFailedException(CreateOrderPaymentRequest req) throws StripeException {
        // Arrange
        when(stripeService.createPayment(any(StripeOrderPaymentDto.class)))
                .thenThrow(mock(StripeException.class));

        // Act & Assert
        assertThrows(PaymentFailedException.class,
                () -> paymentService.createOrderPayment(req));

        verify(stripeService, times(1)).createPayment(any(StripeOrderPaymentDto.class));
        verify(paymentRepository, never()).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createCreateOrderPaymentRequest")
    void createOrderPayment_RepositoryThrowsException_PropagatesDataAccessException(CreateOrderPaymentRequest req) throws StripeException {
        // Arrange
        when(stripeService.createPayment(any(StripeOrderPaymentDto.class)))
                .thenReturn(UUID.randomUUID().toString());
        when(paymentRepository.save(any(PaymentEntity.class)))
                .thenThrow(mock(DataIntegrityViolationException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> paymentService.createOrderPayment(req));

        verify(stripeService, times(1)).createPayment(any(StripeOrderPaymentDto.class));
        verify(paymentRepository, times(1)).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createCreateOrderPaymentRequest")
    void createOrderPayment_ValidRequest_SavesPaymentWithCorrectData(CreateOrderPaymentRequest req) throws StripeException {
        // Arrange
        when(stripeService.createPayment(any(StripeOrderPaymentDto.class)))
                .thenReturn(UUID.randomUUID().toString());
        doAnswer(invocation -> {
            PaymentEntity entity = invocation.getArgument(0);
            entity.setId(UUID.randomUUID());
            return entity;
        }).when(paymentRepository).save(any(PaymentEntity.class));

        // Act
        paymentService.createOrderPayment(req);

        // Assert
        ArgumentCaptor<PaymentEntity> paymentCaptor = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(paymentRepository).save(paymentCaptor.capture());

        assertThat(paymentCaptor.getValue().getId()).isNotNull();
        assertThat(paymentCaptor.getValue().getOrderId()).isEqualTo(req.orderId());
        assertThat(paymentCaptor.getValue().getStripeId()).isNotNull();
        assertThat(paymentCaptor.getValue().getAmount()).isEqualTo(req.amount());

        verify(stripeService, times(1)).createPayment(any(StripeOrderPaymentDto.class));
        verify(paymentRepository, times(1)).save(any(PaymentEntity.class));
    }

    private static Stream<Arguments> createCreateOrderPaymentRequest() {
        return Stream.of(
                Arguments.of(new CreateOrderPaymentRequest(UUID.randomUUID().toString(),
                        UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("500.00")))
        );
    }

    @ParameterizedTest @NullSource
    void topUpBalance_RequestIsNull_ThrowsNpe(UserBalanceRequest req) throws StripeException {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> paymentService.topUpBalance(req));

        verify(stripeService, never()).createTopUpPayment(any(StripeUserPaymentDto.class));
        verify(paymentRepository, never()).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createUserBalanceRequest")
    void topUpBalance_StripeServiceThrowsException_ThrowsPaymentFailedException(UserBalanceRequest req) throws StripeException {
        // Arrange
        when(stripeService.createTopUpPayment(any(StripeUserPaymentDto.class)))
                .thenThrow(mock(StripeException.class));

        // Act & Assert
        assertThrows(PaymentFailedException.class,
                () -> paymentService.topUpBalance(req));

        verify(stripeService, times(1)).createTopUpPayment(any(StripeUserPaymentDto.class));
        verify(paymentRepository, never()).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createUserBalanceRequest")
    void topUpBalance_RepositoryThrowsException_PropagatesDataAccessException(UserBalanceRequest req) throws StripeException {
        // Arrange
        when(stripeService.createTopUpPayment(any(StripeUserPaymentDto.class)))
                .thenReturn(UUID.randomUUID().toString());
        when(paymentRepository.save(any(PaymentEntity.class)))
                .thenThrow(mock(DataIntegrityViolationException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> paymentService.topUpBalance(req));

        verify(stripeService, times(1)).createTopUpPayment(any(StripeUserPaymentDto.class));
        verify(paymentRepository, times(1)).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createUserBalanceRequest")
    void topUpBalance_ValidRequest_SavesPaymentWithCorrectData(UserBalanceRequest req) throws StripeException {
        // Arrange
        when(stripeService.createTopUpPayment(any(StripeUserPaymentDto.class)))
                .thenReturn(UUID.randomUUID().toString());
        doAnswer(invocation -> {
            PaymentEntity entity = invocation.getArgument(0);
            entity.setId(UUID.randomUUID());
            return entity;
        }).when(paymentRepository).save(any(PaymentEntity.class));

        // Act
        paymentService.topUpBalance(req);

        // Assert
        ArgumentCaptor<PaymentEntity> paymentCaptor = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(paymentRepository).save(paymentCaptor.capture());

        assertThat(paymentCaptor.getValue().getId()).isNotNull();
        assertThat(paymentCaptor.getValue().getUserId()).isEqualTo(req.userId());
        assertThat(paymentCaptor.getValue().getStripeId()).isNotNull();
        assertThat(paymentCaptor.getValue().getAmount()).isEqualTo(req.amount());

        verify(stripeService, times(1)).createTopUpPayment(any(StripeUserPaymentDto.class));
        verify(paymentRepository, times(1)).save(any(PaymentEntity.class));
    }

    private static Stream<Arguments> createUserBalanceRequest() {
        return Stream.of(
                Arguments.of(new UserBalanceRequest(UUID.randomUUID().toString(),
                        UUID.randomUUID(), new BigDecimal("500.00")))
        );
    }

    @ParameterizedTest @NullSource
    void refundOrderPayment_RequestIsNull_ThrowsNpe(RefundPaymentRequest req) throws StripeException {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> paymentService.refundOrderPayment(req));

        verify(paymentRepository, never()).findByOrderId(any(UUID.class));
        verify(stripeService, never()).createRefundPayment(any(StripeRefundDto.class));
        verify(paymentRepository, never()).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createRefundPaymentRequest")
    void refundOrderPayment_OriginalPaymentNotFound_ThrowsNotFoundException(RefundPaymentRequest req) throws StripeException {
        // Arrange
        when(paymentRepository.findByOrderId(any(UUID.class)))
                .thenReturn(null);

        // Act & Assert
        assertThrows(PaymentNotFoundByOrderIdException.class,
                () -> paymentService.refundOrderPayment(req));

        verify(paymentRepository, times(1)).findByOrderId(any(UUID.class));
        verify(stripeService, never()).createRefundPayment(any(StripeRefundDto.class));
        verify(paymentRepository, never()).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createRefundPaymentRequest")
    void refundOrderPayment_StripeServiceThrowsException_ThrowsPaymentFailedException(RefundPaymentRequest req) throws StripeException {
        // Arrange
        when(paymentRepository.findByOrderId(any(UUID.class)))
                .thenReturn(mock(PaymentEntity.class));
        when(stripeService.createRefundPayment(any(StripeRefundDto.class)))
                .thenThrow(mock(StripeException.class));

        // Act & Assert
        assertThrows(PaymentFailedException.class,
                () -> paymentService.refundOrderPayment(req));

        verify(paymentRepository, times(1)).findByOrderId(any(UUID.class));
        verify(stripeService, times(1)).createRefundPayment(any(StripeRefundDto.class));
        verify(paymentRepository, never()).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createRefundPaymentRequest")
    void refundOrderPayment_RepositorySaveFails_PropagatesDataAccessException(RefundPaymentRequest req) throws StripeException {
        // Arrange
        when(paymentRepository.findByOrderId(any(UUID.class)))
                .thenReturn(mock(PaymentEntity.class));
        when(stripeService.createRefundPayment(any(StripeRefundDto.class)))
                .thenReturn(UUID.randomUUID().toString());
        when(paymentRepository.save(any(PaymentEntity.class)))
                .thenThrow(mock(DataIntegrityViolationException.class));

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> paymentService.refundOrderPayment(req));

        verify(paymentRepository, times(1)).findByOrderId(any(UUID.class));
        verify(stripeService, times(1)).createRefundPayment(any(StripeRefundDto.class));
        verify(paymentRepository, times(1)).save(any(PaymentEntity.class));
    }

    @ParameterizedTest @MethodSource("createRefundPaymentRequest")
    void refundOrderPayment_ValidRequest_SavesRefundWithCorrectData(RefundPaymentRequest req) throws StripeException {
        // Arrange
        PaymentEntity payment = PaymentEntity.builder()
                .userId(UUID.randomUUID())
                .orderId(UUID.randomUUID())
                .amount(new BigDecimal("500.00"))
                .build();

        when(paymentRepository.findByOrderId(any(UUID.class)))
                .thenReturn(payment);
        when(stripeService.createRefundPayment(any(StripeRefundDto.class)))
                .thenReturn(UUID.randomUUID().toString());
        doAnswer(invocation -> {
            PaymentEntity entity = invocation.getArgument(0);
            entity.setId(UUID.randomUUID());
            return entity;
        }).when(paymentRepository).save(any(PaymentEntity.class));

        // Act
        paymentService.refundOrderPayment(req);

        // Assert
        ArgumentCaptor<PaymentEntity> paymentCaptor = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(paymentRepository).save(paymentCaptor.capture());

        assertThat(paymentCaptor.getValue().getId()).isNotNull();
        assertThat(paymentCaptor.getValue().getUserId()).isEqualTo(payment.getUserId());
        assertThat(paymentCaptor.getValue().getOrderId()).isEqualTo(payment.getOrderId());
        assertThat(paymentCaptor.getValue().getStripeId()).isNotNull();
        assertThat(paymentCaptor.getValue().getAmount()).isEqualTo(payment.getAmount());

        verify(paymentRepository, times(1)).findByOrderId(any(UUID.class));
        verify(stripeService, times(1)).createRefundPayment(any(StripeRefundDto.class));
        verify(paymentRepository, times(1)).save(any(PaymentEntity.class));
    }

    private static Stream<Arguments> createRefundPaymentRequest() {
        return Stream.of(
                Arguments.of(new RefundPaymentRequest(UUID.randomUUID().toString(), UUID.randomUUID()))
        );
    }
}
