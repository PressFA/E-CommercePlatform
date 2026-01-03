package by.pressf.paymentms.service;

import by.pressf.paymentms.dao.entity.PaymentEntity;
import by.pressf.paymentms.dao.entity.type.PaymentType;
import by.pressf.paymentms.dao.repository.PaymentRepository;
import by.pressf.paymentms.dto.CreateOrderPaymentRequest;
import by.pressf.paymentms.exception.PaymentFailedException;
import com.stripe.exception.StripeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

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

    public void refundOrderPayment(UUID orderId) {
        try {
            PaymentEntity payment = paymentRepository.findByOrderId(orderId);

            log.info("Sending a refund to the bank gateway");
            stripeService.createRefundPayment(payment.getAmount());
            log.info("The refund sent to the bank gateway was successfully completed");

            PaymentEntity createPayment = PaymentEntity.builder()
                    .userId(payment.getUserId())
                    .orderId(payment.getOrderId())
                    .amount(payment.getAmount())
                    .createdAt(LocalDateTime.now())
                    .type(PaymentType.REFUND)
                    .build();

            paymentRepository.save(createPayment);
        } catch (StripeException e) {
            throw new PaymentFailedException("A payment gateway error has occurred.", e.getStatusCode());
        }
    }
}
