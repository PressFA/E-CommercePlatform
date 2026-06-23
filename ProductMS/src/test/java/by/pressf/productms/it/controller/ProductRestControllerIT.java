package by.pressf.productms.it.controller;

import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dto.incoming.CreateProductRequest;
import by.pressf.productms.dto.incoming.PatchProductRequest;
import by.pressf.productms.it.config.BaseIT;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class ProductRestControllerIT extends BaseIT {
    @BeforeEach
    void setUp() { productRepository.deleteAll(); }

    @Test
    void should_CreateProduct_When_CreateProductRequestIsValid() throws Exception {
        // Arrange
        CreateProductRequest request = new CreateProductRequest(
                "Gaming Mouse",
                10,
                new BigDecimal("45.50")
        );

        // Act
        MvcResult result = mockMvc.perform(post("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isCreated(),
                        jsonPath("$.productId").exists()
                )
                .andReturn();

        // Assert
        String jsonResponse = result.getResponse().getContentAsString();
        String productIdStr = mapper.readTree(jsonResponse).get("productId").asText();
        UUID productId = UUID.fromString(productIdStr);

        ProductEntity savedProduct = productRepository.findById(productId)
                .orElseThrow(() -> new AssertionError("Товар не был сохранен в БД"));

        assertThat(savedProduct.getName()).isEqualTo("Gaming Mouse");
        assertThat(savedProduct.getQuantity()).isEqualTo(10);
        assertThat(savedProduct.getPrice()).isEqualByComparingTo(new BigDecimal("45.50"));
    }

    @Test
    void should_ReturnBadRequest_When_CreateProductRequestIsInvalid() throws Exception {
        // Arrange
        CreateProductRequest invalidRequest = new CreateProductRequest(
                "",
                150,
                new BigDecimal("0.50")
        );

        // Act & Assert
        mockMvc.perform(post("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpect(status().isBadRequest());

        assertThat(productRepository.count()).isZero();
    }

    @Test
    void should_PatchProduct_When_PatchProductRequestIsValid() throws Exception {
        // Arrange
        ProductEntity product = productRepository.save(ProductEntity.builder()
                .name("Keyboard")
                .quantity(50)
                .price(new BigDecimal("100.00"))
                .version(0)
                .build());

        PatchProductRequest patchRequest = new PatchProductRequest(
                product.getId(),
                20,
                new BigDecimal("120.00")
        );

        // Act
        mockMvc.perform(patch("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(patchRequest)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(product.getId().toString()))
                .andExpect(jsonPath("$.name").value("Keyboard"))
                .andExpect(jsonPath("$.quantity").value(70))
                .andExpect(jsonPath("$.price").value(120.00));

        // Assert
        ProductEntity updatedProduct = productRepository.findById(product.getId())
                .orElseThrow(() -> new AssertionError("Товар не найден в БД"));

        assertThat(updatedProduct.getQuantity()).isEqualTo(70);
        assertThat(updatedProduct.getPrice()).isEqualByComparingTo(new BigDecimal("120.00"));
    }

    @Test
    void should_ReturnNotFound_When_ProductToPatchDoesNotExist() throws Exception {
        // Arrange
        PatchProductRequest patchRequest = new PatchProductRequest(
                UUID.randomUUID(),
                5,
                new BigDecimal("50.00")
        );

        // Act & Assert
        mockMvc.perform(patch("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(patchRequest)))
                .andExpect(status().isNotFound());
    }
}
