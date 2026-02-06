package by.pressf.paymentms.service;

import by.pressf.paymentms.dao.entity.PaymentEntity;
import by.pressf.paymentms.dao.entity.type.PaymentType;
import by.pressf.paymentms.dao.repository.PaymentRepository;
import by.pressf.paymentms.dto.*;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.exception.PaymentNotFoundByOrderIdException;
import com.stripe.exception.StripeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {
    private final StripeService stripeService;
    private final PaymentRepository paymentRepository;

    public void createOrderPayment(CreateOrderPaymentRequest req) {
        try {
            log.info("Sending a payment to a bank gateway");
            String stripeId = stripeService.createPayment(new StripeOrderPaymentDto(req.idempotencyKey(), req.amount()));
            log.info("The payment sent to the bank gateway was successful");

            PaymentEntity payment = PaymentEntity.builder()
                    .userId(req.userId())
                    .orderId(req.orderId())
                    .stripeId(stripeId)
                    .amount(req.amount())
                    .type(PaymentType.PAYMENT)
                    .build();

            paymentRepository.save(payment);
        } catch (StripeException e) {
            log.error(e.getMessage());
            throw new PaymentFailedException("A payment gateway error has occurred.", e.getStatusCode());
        }
    }

    public void refundOrderPayment(RefundPaymentRequest req) {
        try {
            PaymentEntity payment = paymentRepository.findByOrderId(req.orderId());

            if (payment == null) {
                throw new PaymentNotFoundByOrderIdException(req.orderId());
            }

            log.warn("Sending a refund to the bank gateway");
            String stripeId = stripeService.createRefundPayment(new StripeRefundDto(req.idempotencyKey(), payment.getStripeId()));
            log.warn("The refund sent to the bank gateway was successfully completed");

            PaymentEntity createPayment = PaymentEntity.builder()
                    .userId(payment.getUserId())
                    .orderId(payment.getOrderId())
                    .stripeId(stripeId)
                    .amount(payment.getAmount())
                    .type(PaymentType.REFUND)
                    .build();

            paymentRepository.save(createPayment);
        } catch (StripeException e) {
            log.error(e.getMessage());
            throw new PaymentFailedException("A payment gateway error has occurred.", e.getStatusCode());
        }
    }

    public void topUpBalance(UserBalanceRequest req) {
        try {
            log.info("Sending a balance top-up request to the bank gateway");
            String stripeId = stripeService.createTopUpPayment(new StripeUserPaymentDto(req.idempotencyKey(), req.amount()));
            log.info("The balance top-up request to the bank gateway was successful");

            PaymentEntity payment = PaymentEntity.builder()
                    .userId(req.userId())
                    .stripeId(stripeId)
                    .amount(req.amount())
                    .type(PaymentType.TOP_UP)
                    .build();

            paymentRepository.save(payment);
        } catch (StripeException e) {
            log.error(e.getMessage());
            throw new PaymentFailedException("A payment gateway error has occurred.", e.getStatusCode());
        }
    }
}
