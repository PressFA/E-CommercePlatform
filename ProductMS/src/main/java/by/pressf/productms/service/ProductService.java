package by.pressf.productms.service;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.repository.ProductRepository;
import by.pressf.productms.dto.internal.ProductCreationData;
import by.pressf.productms.dto.internal.ProductData;
import by.pressf.productms.dto.internal.ProductPatchingData;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.exception.ProductOverflowException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {
    private final ProductRepository productRepository;

    @Transactional("transactionManager")
    public UUID createProduct(@NonNull ProductCreationData creationData) {
        Objects.requireNonNull(creationData, "ProductCreationData must not be null");

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
    public ProductData patchProduct(@NonNull ProductPatchingData patchingData) {
        Objects.requireNonNull(patchingData, "ProductPatchingData must not be null");

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
}
