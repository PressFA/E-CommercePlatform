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
        ProductEntity product = productRepository.findById(req.productId())
                .orElseThrow(() -> new ProductNotFoundException(req.productId()));

        if (req.quantity() > product.getQuantity()) {
            throw new ProductInsufficientException(req.productId(), req.orderId());
        }

        product.setQuantity(product.getQuantity() - req.quantity());
        productRepository.save(product);

        ProductHistoryEntity productHistory = ProductHistoryEntity.builder()
                .orderId(req.orderId())
                .product(product)
                .quantity(req.quantity())
                .createdAt(LocalDateTime.now())
                .status(ProductStatus.RESERVED)
                .build();
        productHistoryRepository.save(productHistory);

        return product.getPrice().multiply(new BigDecimal(req.quantity()));
    }

    public void confirmProductOrder(UUID orderId) {
        ProductHistoryEntity entity = productHistoryRepository.findByOrderId(orderId);

        entity.setStatus(ProductStatus.CONFIRMED);
        entity.setUpdatedAt(LocalDateTime.now());

        productHistoryRepository.save(entity);
    }

    public void cancelProductReservation(UUID orderId) {
        ProductHistoryEntity productHistory = productHistoryRepository.findByOrderId(orderId);

        ProductEntity product = productRepository.findById(productHistory.getProduct().getId())
                .orElseThrow(() -> new ProductNotFoundException(productHistory.getProduct().getId()));

        product.setQuantity(product.getQuantity() + productHistory.getQuantity());
        productRepository.save(product);

        productHistory.setStatus(ProductStatus.CANCELLED);
        productHistory.setUpdatedAt(LocalDateTime.now());

        productHistoryRepository.save(productHistory);
    }
}
