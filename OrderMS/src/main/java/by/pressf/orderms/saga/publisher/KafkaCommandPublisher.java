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
import org.jspecify.annotations.NonNull;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaCommandPublisher {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // ForwardSagaHandler
    public void sendReserveProductCommand(@NonNull String keyMessage,
                                          @NonNull ReserveProductCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("product.commands.topic.name"), keyMessage, valueMessage);
    }

    // ForwardSagaHandler
    public void sendChargePaymentCommand(@NonNull String keyMessage,
                                         @NonNull ChargePaymentCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("payment.commands.topic.name"), keyMessage, valueMessage);
    }

    // ForwardSagaHandler
    public void sendDebitUserBalanceCommand(@NonNull String keyMessage,
                                            @NonNull DebitUserBalanceCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("user.commands.topic.name"), keyMessage, valueMessage);
    }

    // ForwardSagaHandler
    public void sendConfirmOrderCommand(@NonNull String keyMessage,
                                        @NonNull ConfirmOrderCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("order.commands.topic.name"), keyMessage, valueMessage);
    }

    // CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendSendEmailOrderCommand(@NonNull String keyMessage,
                                          @NonNull SendEmailOrderCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("email-notification.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler & CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendRefundPaymentCommand(@NonNull String keyMessage,
                                         @NonNull RefundPaymentCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("payment.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler
    public void sendCancelUserBalanceDebitCommand(@NonNull String keyMessage,
                                                  @NonNull CancelUserBalanceDebitCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("user.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler & CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendRejectOrderCommand(@NonNull String keyMessage,
                                       @NonNull RejectOrderCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("order.commands.topic.name"), keyMessage, valueMessage);
    }

    // CompensationSagaHandler & CriticalAuditSagaHandler & RollbackSagaHandler
    public void sendCancelProductReservationCommand(@NonNull String keyMessage,
                                                    @NonNull CancelProductReservationCommand valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("product.commands.topic.name"), keyMessage, valueMessage);
    }

    private void send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The {} message was sent to the {} topic.", value.getClass().getSimpleName(), topic);
    }
}
