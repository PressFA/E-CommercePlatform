package by.pressf.productms.controller;

import by.pressf.productms.dto.CreateProductRequest;
import by.pressf.productms.dto.ProductCreationData;
import by.pressf.productms.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/product")
public class ProductRestController {
    private final ProductService productService;

    @PostMapping
    public ResponseEntity<?> createProduct(@RequestBody CreateProductRequest req) {
        log.info("A request was received to add a product with the name {} to the catalog", req.name());

        ProductCreationData productCreationData = new ProductCreationData(
                req.name(),
                req.quantity(),
                req.price()
        );

        UUID productId = productService.createProduct(productCreationData);

        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }
}
