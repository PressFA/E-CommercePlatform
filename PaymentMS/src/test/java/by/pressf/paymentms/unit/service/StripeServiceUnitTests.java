package by.pressf.paymentms.unit.service;

import by.pressf.paymentms.dto.StripeOrderPaymentDto;
import by.pressf.paymentms.dto.StripeRefundDto;
import by.pressf.paymentms.dto.StripeUserPaymentDto;
import by.pressf.paymentms.service.StripeService;
import com.stripe.exception.AuthenticationException;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.RefundCreateParams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StripeServiceUnitTests {
    @InjectMocks
    private StripeService stripeService;

    @ParameterizedTest @NullSource
    void createPayment_RequestIsNull_ThrowsNpe(StripeOrderPaymentDto dto) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> stripeService.createPayment(dto));
    }

    @Test
    void createPayment_StripeApiAuthenticationFails_ThrowsStripeException() {
        // Arrange
        StripeOrderPaymentDto dto
                = new StripeOrderPaymentDto(UUID.randomUUID().toString(), new BigDecimal("99.99"));

        try (MockedStatic<PaymentIntent> utilIntent = Mockito.mockStatic(PaymentIntent.class)) {
            utilIntent.when(() -> PaymentIntent.create(any(PaymentIntentCreateParams.class), any(RequestOptions.class)))
                    .thenThrow(mock(AuthenticationException.class));

            // Act & Assert
            assertThrows(StripeException.class,
                    () -> stripeService.createPayment(dto));
        }
    }

    @Test
    void createPayment_ValidRequest_CallsStripeApiWithCorrectParams() throws StripeException {
        // Arrange
        StripeOrderPaymentDto dto
                = new StripeOrderPaymentDto(UUID.randomUUID().toString(), new BigDecimal("99.99"));

        try (MockedStatic<PaymentIntent> utilIntent = Mockito.mockStatic(PaymentIntent.class)) {
            PaymentIntent mockIntent = mock(PaymentIntent.class);
            when(mockIntent.getId()).thenReturn("pi_3MtwBwLkdIwHu7ix28a3tqPa");

            utilIntent.when(() -> PaymentIntent.create(any(PaymentIntentCreateParams.class), any(RequestOptions.class)))
                    .thenReturn(mockIntent);

            // Act
            String stripeId = stripeService.createPayment(dto);

            // Assert
            ArgumentCaptor<PaymentIntentCreateParams> paramsCaptor = ArgumentCaptor.forClass(PaymentIntentCreateParams.class);
            ArgumentCaptor<RequestOptions> optionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);

            utilIntent.verify(() -> PaymentIntent.create(paramsCaptor.capture(), optionsCaptor.capture()));

            assertThat(stripeId)
                    .isNotNull()
                    .isEqualTo(mockIntent.getId());
            assertThat(optionsCaptor.getValue().getIdempotencyKey()).isNotNull().endsWith(dto.idempotencyKey());
            assertThat(paramsCaptor.getValue().getAmount())
                    .isEqualTo(dto.amount().multiply(new BigDecimal("100")).longValue());
        }
    }

    @ParameterizedTest @NullSource
    void createRefundPayment_RequestIsNull_ThrowsNpe(StripeRefundDto dto) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> stripeService.createRefundPayment(dto));
    }

    @Test
    void createRefundPayment_StripeApiFails_ThrowsStripeException() {
        // Arrange
        StripeRefundDto dto = new StripeRefundDto(UUID.randomUUID().toString(), "pi_123");

        try (MockedStatic<Refund> utilRefund = Mockito.mockStatic(Refund.class)) {
            utilRefund.when(() -> Refund.create(any(RefundCreateParams.class), any(RequestOptions.class)))
                    .thenThrow(mock(AuthenticationException.class));

            // Act & Assert
            assertThrows(StripeException.class,
                    () -> stripeService.createRefundPayment(dto));
        }
    }

    @Test
    void createRefundPayment_ValidRequest_CallsStripeApiWithCorrectParams() throws StripeException {
        // Arrange
        StripeRefundDto dto = new StripeRefundDto(UUID.randomUUID().toString(), "pi_original_payment");

        try (MockedStatic<Refund> utilRefund = Mockito.mockStatic(Refund.class)) {
            Refund mockRefund = mock(Refund.class);
            when(mockRefund.getId()).thenReturn("re_123");

            utilRefund.when(() -> Refund.create(any(RefundCreateParams.class), any(RequestOptions.class)))
                    .thenReturn(mockRefund);

            // Act
            String refundId = stripeService.createRefundPayment(dto);

            // Assert
            ArgumentCaptor<RefundCreateParams> paramsCaptor = ArgumentCaptor.forClass(RefundCreateParams.class);
            ArgumentCaptor<RequestOptions> optionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);

            utilRefund.verify(() -> Refund.create(paramsCaptor.capture(), optionsCaptor.capture()));

            assertThat(refundId)
                    .isNotNull()
                    .isEqualTo(mockRefund.getId());
            assertThat(paramsCaptor.getValue().getPaymentIntent()).isEqualTo(dto.stripeId());
            assertThat(optionsCaptor.getValue().getIdempotencyKey()).endsWith(dto.idempotencyKey());
        }
    }

    @ParameterizedTest @NullSource
    void createTopUpPayment_RequestIsNull_ThrowsNpe(StripeUserPaymentDto dto) {
        // Arrange & Act & Assert
        assertThrows(NullPointerException.class,
                () -> stripeService.createTopUpPayment(dto));
    }

    @Test
    void createTopUpPayment_StripeApiFails_ThrowsStripeException() {
        StripeUserPaymentDto dto = new StripeUserPaymentDto(UUID.randomUUID().toString(), new BigDecimal("50.00"));

        try (MockedStatic<PaymentIntent> utilIntent = Mockito.mockStatic(PaymentIntent.class)) {
            utilIntent.when(() -> PaymentIntent.create(any(PaymentIntentCreateParams.class), any(RequestOptions.class)))
                    .thenThrow(mock(AuthenticationException.class));

            assertThrows(StripeException.class,
                    () -> stripeService.createTopUpPayment(dto));
        }
    }

    @Test
    void createTopUpPayment_ValidRequest_CallsStripeApiWithCorrectParams() throws StripeException {
        // Arrange
        StripeUserPaymentDto dto = new StripeUserPaymentDto(UUID.randomUUID().toString(), new BigDecimal("50.00"));

        try (MockedStatic<PaymentIntent> utilIntent = Mockito.mockStatic(PaymentIntent.class)) {
            PaymentIntent mockIntent = mock(PaymentIntent.class);
            when(mockIntent.getId()).thenReturn("pi_top-up_123");

            utilIntent.when(() -> PaymentIntent.create(any(PaymentIntentCreateParams.class), any(RequestOptions.class)))
                    .thenReturn(mockIntent);

            // Act
            String stripeId = stripeService.createTopUpPayment(dto);

            // Assert
            ArgumentCaptor<PaymentIntentCreateParams> paramsCaptor = ArgumentCaptor.forClass(PaymentIntentCreateParams.class);
            ArgumentCaptor<RequestOptions> optionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);

            utilIntent.verify(() -> PaymentIntent.create(paramsCaptor.capture(), optionsCaptor.capture()));

            assertThat(stripeId)
                    .isNotNull()
                    .isEqualTo(mockIntent.getId());
            assertThat(optionsCaptor.getValue().getIdempotencyKey()).endsWith(dto.idempotencyKey());
            assertThat(paramsCaptor.getValue().getAmount())
                    .isEqualTo(dto.amount().multiply(new BigDecimal("100")).longValue());
        }
    }
}
