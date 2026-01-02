package by.pressf.paymentms.service;

import by.pressf.paymentms.dao.entity.PaymentEntity;
import by.pressf.paymentms.dao.entity.types.PaymentType;
import by.pressf.paymentms.dao.repository.PaymentRepository;
import by.pressf.paymentms.dto.CreateOrderPaymentRequest;
import by.pressf.paymentms.exception.PaymentFailedException;
import com.stripe.exception.StripeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {
    private final StripeService stripeService;
    private final PaymentRepository paymentRepository;

    public void createOrderPayment(CreateOrderPaymentRequest req) {
        try {
            log.info("Sending a payment to a bank gateway");
            stripeService.createPayment(req.amount());
            log.info("The payment sent to the bank gateway was successful");

            PaymentEntity payment = PaymentEntity.builder()
                    .userId(req.userId())
                    .orderId(req.orderId())
                    .amount(req.amount())
                    .createdAt(LocalDateTime.now())
                    .type(PaymentType.PAYMENT)
                    .build();

            paymentRepository.save(payment);
        } catch (StripeException e) {
            throw new PaymentFailedException("A payment gateway error has occurred.", e.getStatusCode());
        }
    }
}
