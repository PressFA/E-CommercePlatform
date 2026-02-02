package by.pressf.productms.service.handler;

import by.pressf.core.dto.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.commands.product.ReserveProductCommand;
import by.pressf.core.dto.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.events.product.ProductReservedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.productms.dao.entity.EventEntity;
import by.pressf.productms.dao.repository.EventRepository;
import by.pressf.productms.dto.ProductReservationRequest;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundByOrderIdException;
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
@KafkaListener(topics = "${product.commands.topic.name}", groupId = "product-ms")
public class ProductCommandsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProductService productService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
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
                    command.username(),
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

            ProductReservationFailedEvent failedEvent = createFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("product.events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (ProductNotFoundByOrderIdException | ProductInsufficientException | DataAccessException e) {
            log.error(e.getMessage());

            ProductReservationFailedEvent failedEvent = createFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("product.events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload CancelProductReservationCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The CancelProductReservationCommand command from the product-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The CancelProductReservationCommand message with messageId={} has already been processed", messageId);
                return;
            }

            productService.cancelProductReservation(command.orderId());
            log.info("The product from the order with ID {} has been removed from the reservation", command.orderId());

            ProductReservationCanceledEvent event = new ProductReservationCanceledEvent(command.orderId(),command.username());
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("product.events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The ProductReservationCanceledEvent message was sent to the product-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The CancelProductReservationCommand message with messageId={} has been processed", messageId);
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());

            ProductReservationCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("product.events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (ProductNotFoundByOrderIdException | DataAccessException e) {
            log.error(e.getMessage());

            ProductReservationCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("product.events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    private ProductReservationFailedEvent createFailedEvent(ReserveProductCommand command) {
        return new ProductReservationFailedEvent(command.orderId(), command.username());
    }

    private ProductReservationCancelFailedEvent createCancelFailedEvent(CancelProductReservationCommand command) {
        return new ProductReservationCancelFailedEvent(command.orderId(), command.username());
    }
}
