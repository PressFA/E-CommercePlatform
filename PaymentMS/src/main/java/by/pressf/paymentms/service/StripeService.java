package by.pressf.paymentms.service;

import by.pressf.paymentms.dto.StripePaymentDto;
import by.pressf.paymentms.dto.StripeRefundDto;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.RefundCreateParams;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class StripeService {
    public String createPayment(StripePaymentDto dto) throws StripeException {
        PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
                .setAmount(dto.amount().multiply(new BigDecimal("100")).longValue())
                .setCurrency("usd")
                .addPaymentMethodType("card")
                .build();

        RequestOptions options = RequestOptions.builder()
                .setIdempotencyKey("payment_" + dto.orderId())
                .build();

        PaymentIntent paymentIntent = PaymentIntent.create(params, options);
        return paymentIntent.getId();
    }

    public void createRefundPayment(StripeRefundDto dto) throws StripeException {
        RefundCreateParams params = RefundCreateParams.builder()
                .setPaymentIntent(dto.stripeId())
                .build();

        RequestOptions options = RequestOptions.builder()
                .setIdempotencyKey("refund_" + dto.orderId())
                .build();

        Refund.create(params, options);
    }
}
