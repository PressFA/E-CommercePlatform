package by.pressf.productms.kafka.listener;

import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.productms.exception.ProductHistoryNotFoundException;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundByOrderIdException;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.kafka.handler.ProductCommandsHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${product.commands.topic.name}", groupId = "product-ms")
public class ProductCommandsListener {
    private final Environment env;
    private final ProductCommandsHandler handler;

    @KafkaHandler
    public void handleCommand(@Payload ReserveProductCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The ReserveProductCommand command from the product-commands topic has been received");

            handler.handleReserveProductCommand(command, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());

            ProductReservationFailedEvent failedEvent = createFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (ProductNotFoundException | ProductInsufficientException | DataAccessException e) {
            log.error(e.getMessage());

            ProductReservationFailedEvent failedEvent = createFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    public void handleCommand(@Payload CancelProductReservationCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.warn("The CancelProductReservationCommand command from the product-commands topic has been received");

            handler.handleCancelProductReservationCommand(command, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());

            ProductReservationCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (ProductHistoryNotFoundException | ProductNotFoundByOrderIdException | DataAccessException e) {
            log.error(e.getMessage());

            ProductReservationCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
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
