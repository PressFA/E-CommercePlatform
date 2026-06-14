package by.pressf.orderms.saga.publisher;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.NullMarked;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@NullMarked
@RequiredArgsConstructor
public class KafkaCommandPublisher {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // ForwardSagaHandler
    public void sendReserveProductCommand(String keyMessage, ReserveProductCommand valueMessage) {
        send(env.getRequiredProperty("product.commands.topic.name"), keyMessage, valueMessage);
    }

    // ForwardSagaHandler
    public void sendChargePaymentCommand(String keyMessage, ChargePaymentCommand valueMessage) {
        send(env.getRequiredProperty("payment.commands.topic.name"), keyMessage, valueMessage);
    }

    // ForwardSagaHandler
    public void sendDebitUserBalanceCommand(String keyMessage, DebitUserBalanceCommand valueMessage) {
        send(env.getRequiredProperty("user.commands.topic.name"), keyMessage, valueMessage);
    }

    // ForwardSagaHandler
    public void sendConfirmOrderCommand(String keyMessage, ConfirmOrderCommand valueMessage) {
        send(env.getRequiredProperty("order.commands.topic.name"), keyMessage, valueMessage);
    }

    // CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendSendEmailOrderCommand(String keyMessage, SendEmailOrderCommand valueMessage) {
        send(env.getRequiredProperty("email-notification.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler & CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendRefundPaymentCommand(String keyMessage, RefundPaymentCommand valueMessage) {
        send(env.getRequiredProperty("payment.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler
    public void sendCancelUserBalanceDebitCommand(String keyMessage, CancelUserBalanceDebitCommand valueMessage) {
        send(env.getRequiredProperty("user.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler & CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendRejectOrderCommand(String keyMessage, RejectOrderCommand valueMessage) {
        send(env.getRequiredProperty("order.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler & CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendCancelProductReservationCommand(String keyMessage, CancelProductReservationCommand valueMessage) {
        send(env.getRequiredProperty("product.commands.topic.name"), keyMessage, valueMessage);
    }

    private void send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The {} message was sent to the {} topic.", value.getClass().getSimpleName(), topic);
    }
}
