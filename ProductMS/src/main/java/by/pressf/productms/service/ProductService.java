package by.pressf.productms.service;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.entity.ProductHistoryEntity;
import by.pressf.productms.dao.entity.status.ProductStatus;
import by.pressf.productms.dao.repository.ProductHistoryRepository;
import by.pressf.productms.dao.repository.ProductRepository;
import by.pressf.productms.dto.ProductReservationRequest;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProductService {
    private final ProductRepository productRepository;
    private final ProductHistoryRepository productHistoryRepository;

    public BigDecimal reserveProduct(ProductReservationRequest req) {
        ProductEntity entity = productRepository.findById(req.productId())
                .orElseThrow(() -> new ProductNotFoundException(req.productId()));

        if (req.quantity() > entity.getQuantity()) {
            throw new ProductInsufficientException(req.productId(), req.orderId());
        }

        entity.setQuantity(entity.getQuantity() - req.quantity());
        productRepository.save(entity);

        ProductHistoryEntity historyEntity = ProductHistoryEntity.builder()
                .orderId(req.orderId())
                .product(entity)
                .quantity(req.quantity())
                .createdAt(LocalDateTime.now())
                .status(ProductStatus.RESERVED)
                .build();
        productHistoryRepository.save(historyEntity);

        return entity.getPrice().multiply(new BigDecimal(req.quantity()));
    }

    public void confirmProductOrder(UUID orderId) {
        ProductHistoryEntity entity = productHistoryRepository.findByOrderId(orderId);

        entity.setStatus(ProductStatus.CONFIRMED);
        entity.setUpdatedAt(LocalDateTime.now());

        productHistoryRepository.save(entity);
    }
}
