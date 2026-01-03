package by.pressf.paymentms.service;

import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
public class StripeService {
    public void createPayment(BigDecimal amount) throws StripeException {
        Map<String, Object> params = Map.of(
                "amount", amount.multiply(new BigDecimal("100")),
                "currency", "usd",
                "payment_method_types", java.util.List.of("card")
        );

        PaymentIntent.create(params);
    }

    public void createRefundPayment(BigDecimal amount) throws StripeException {
        Map<String, Object> params = Map.of(
                "amount", amount.multiply(new BigDecimal("100")),
                "currency", "usd",
                "payment_method_types", java.util.List.of("card")
        );

        PaymentIntent.create(params);
    }
}
