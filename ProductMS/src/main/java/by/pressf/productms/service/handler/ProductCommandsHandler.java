package by.pressf.productms.service.handler;

import by.pressf.core.dto.commands.ConfirmOrderCommand;
import by.pressf.core.dto.commands.ReserveProductCommand;
import by.pressf.core.dto.events.ProductReservedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.productms.dao.entity.EventEntity;
import by.pressf.productms.dao.repository.EventRepository;
import by.pressf.productms.dto.ProductReservationRequest;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = {
        "${product.commands.topic.name}",
        "${order.commands.topic.name}"
        }, groupId = "product-ms")
public class ProductCommandsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProductService productService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handleCommand(@Payload ReserveProductCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The ReserveProductCommand command from the product-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ReserveProductCommand message with messageId={} has already been processed", messageId);
                return;
            }

            ProductReservationRequest productReservation = new ProductReservationRequest(
                    command.orderId(),
                    command.productId(),
                    command.quantity()
            );
            BigDecimal totalCostProduct = productService.reserveProduct(productReservation);
            log.info("The product for the order with the ID {} has been successfully reserved", command.orderId());

            ProductReservedEvent event = new ProductReservedEvent(
                    command.orderId(),
                    command.userId(),
                    totalCostProduct
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("product.events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The ProductReservedEvent message was sent to the product-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ReserveProductCommand message with messageId={} has been processed", messageId);
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());
            throw new RetryableException(e);
        } catch (ProductNotFoundException | ProductInsufficientException | DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handleCommand(@Payload ConfirmOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The ConfirmOrderCommand command from the product-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ConfirmOrderCommand message with messageId={} has already been processed", messageId);
                return;
            }

            productService.confirmProductOrder(command.orderId());
            log.info("The order of the product with the ID {} has been confirmed", command.orderId());

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ConfirmOrderCommand message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
