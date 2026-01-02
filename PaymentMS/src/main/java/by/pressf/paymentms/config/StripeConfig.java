package by.pressf.paymentms.config;

import com.stripe.Stripe;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@RequiredArgsConstructor
public class StripeConfig {
    private final Environment env;

    @PostConstruct
    public void init() {
        Stripe.apiKey = env.getRequiredProperty("stripe.api-key");
    }
}
