package by.pressf.productms.controller;

import by.pressf.productms.dto.incoming.CreateProductRequest;
import by.pressf.productms.dto.incoming.PatchProductRequest;
import by.pressf.productms.dto.internal.ProductData;
import by.pressf.productms.service.ProductService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/product")
public class ProductRestController {
    private final ProductService productService;

    @PostMapping
    public ResponseEntity<?> createProduct(@RequestBody @Valid CreateProductRequest req) {
        log.info("A request was received to add a product with the name {} to the catalog", req.name());

        UUID productId = productService.createProduct(req);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("productId", productId));
    }

    @PatchMapping
    public ResponseEntity<?> patchProduct(@RequestBody @Valid PatchProductRequest req) {
        log.info("A request was received to update the quantity and/or price of an item with the {} ID",
                req.productId());

        ProductData data = productService.patchProduct(req);

        return ResponseEntity.status(HttpStatus.OK).body(data);
    }
}
