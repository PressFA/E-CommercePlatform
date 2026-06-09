package by.pressf.paymentms.service;

import by.pressf.paymentms.dto.StripeOrderPaymentDto;
import by.pressf.paymentms.dto.StripeRefundDto;
import by.pressf.paymentms.dto.StripeUserPaymentDto;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.RefundCreateParams;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Objects;

@Slf4j
@Service
public class StripeService {
    public String createPayment(@NonNull StripeOrderPaymentDto dto) throws StripeException {
        Objects.requireNonNull(dto, "StripeOrderPaymentDto must not be null");

        PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
                .setAmount(dto.amount().multiply(new BigDecimal("100")).longValue())
                .setCurrency("usd")
                .setConfirm(true)
                .setPaymentMethod("pm_card_visa")
                .setReturnUrl("http://localhost:8080/success")
                .build();

        RequestOptions options = RequestOptions.builder()
                .setIdempotencyKey("payment_" + dto.idempotencyKey())
                .build();

        PaymentIntent paymentIntent = PaymentIntent.create(params, options);
        log.info("{}", paymentIntent.getStatus());
        return paymentIntent.getId();
    }

    public String createRefundPayment(@NonNull StripeRefundDto dto) throws StripeException {
        Objects.requireNonNull(dto, "StripeRefundDto must not be null");

        RefundCreateParams params = RefundCreateParams.builder()
                .setPaymentIntent(dto.stripeId())
                .build();

        RequestOptions options = RequestOptions.builder()
                .setIdempotencyKey("refund_" + dto.idempotencyKey())
                .build();

        Refund refund = Refund.create(params, options);
        return refund.getId();
    }

    public String createTopUpPayment(@NonNull StripeUserPaymentDto dto) throws StripeException {
        Objects.requireNonNull(dto, "StripeUserPaymentDto must not be null");

        PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
                .setAmount(dto.amount().multiply(new BigDecimal("100")).longValue())
                .setCurrency("usd")
                .addPaymentMethodType("card")
                .build();

        RequestOptions options = RequestOptions.builder()
                .setIdempotencyKey("top-up_" + dto.idempotencyKey())
                .build();

        PaymentIntent paymentIntent = PaymentIntent.create(params, options);
        return paymentIntent.getId();
    }
}
