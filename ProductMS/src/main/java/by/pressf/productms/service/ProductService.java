package by.pressf.productms.service;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.entity.ProductHistoryEntity;
import by.pressf.productms.dao.entity.status.ProductStatus;
import by.pressf.productms.dao.repository.ProductHistoryRepository;
import by.pressf.productms.dao.repository.ProductRepository;
import by.pressf.productms.dto.ProductCreationData;
import by.pressf.productms.dto.ProductData;
import by.pressf.productms.dto.ProductPatchingData;
import by.pressf.productms.dto.ProductReservationRequest;
import by.pressf.productms.exception.ProductInsufficientException;
import by.pressf.productms.exception.ProductNotFoundByOrderIdException;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.exception.ProductOverflowException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {
    private final ProductRepository productRepository;
    private final ProductHistoryRepository productHistoryRepository;

    @Transactional("transactionManager")
    public UUID createProduct(ProductCreationData creationData) {
        ProductEntity productEntity = ProductEntity.builder()
                .name(creationData.name())
                .quantity(creationData.quantity())
                .price(creationData.price())
                .build();
        productRepository.save(productEntity);
        log.info("A new product with the ID {} has been successfully added", productEntity.getId());

        return productEntity.getId();
    }

    @Transactional("transactionManager")
    public ProductData patchProduct(ProductPatchingData patchingData) {
        ProductEntity product = productRepository.findById(patchingData.productId())
                .orElseThrow(() -> new ProductNotFoundException(patchingData.productId()));

        if (patchingData.quantity() != null) {
            int totalQuantity = product.getQuantity() + patchingData.quantity();
            if (totalQuantity <= 100) {
                product.setQuantity(totalQuantity);
            } else {
                throw new ProductOverflowException(totalQuantity);
            }
        }

        if (patchingData.price() != null) {
            product.setPrice(patchingData.price());
        }

        productRepository.save(product);
        log.info("Patch of product with ID {} completed successfully. New quantity: {}, new price: {}",
                product.getId(), product.getQuantity(), product.getPrice());

        return new ProductData(product.getId(), product.getName(), product.getQuantity(), product.getPrice());
    }

    public BigDecimal reserveProduct(ProductReservationRequest req) {
        ProductEntity product = productRepository.findById(req.productId())
                .orElseThrow(() -> new ProductNotFoundByOrderIdException(req.productId()));

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

    public void cancelProductReservation(UUID orderId) {
        ProductHistoryEntity productHistory = productHistoryRepository.findByOrderId(orderId);

        ProductEntity product = productRepository.findById(productHistory.getProduct().getId())
                .orElseThrow(() -> new ProductNotFoundByOrderIdException(productHistory.getProduct().getId()));

        product.setQuantity(product.getQuantity() + productHistory.getQuantity());
        productRepository.save(product);

        productHistory.setStatus(ProductStatus.CANCELLED);
        productHistory.setUpdatedAt(LocalDateTime.now());

        productHistoryRepository.save(productHistory);
    }
}
