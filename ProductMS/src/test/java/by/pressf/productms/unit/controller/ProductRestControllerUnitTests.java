package by.pressf.productms.unit.controller;

import by.pressf.productms.controller.ProductRestController;
import by.pressf.productms.dto.incoming.CreateProductRequest;
import by.pressf.productms.dto.incoming.PatchProductRequest;
import by.pressf.productms.dto.internal.ProductData;
import by.pressf.productms.exception.ProductNotFoundException;
import by.pressf.productms.exception.ProductOverflowException;
import by.pressf.productms.service.ProductService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ProductRestController.class)
public class ProductRestControllerUnitTests {
    @MockitoBean
    private ProductService productService;
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper mapper;

    @ParameterizedTest @MethodSource("provideInvalidProducts")
    void createProduct_InvalidData_ReturnsBadRequest(CreateProductRequest invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(post("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(productService);
    }

    private static Stream<Arguments> provideInvalidProducts() {
        return Stream.of(
                Arguments.of(new CreateProductRequest("", 10, new BigDecimal("100"))),
                Arguments.of(new CreateProductRequest("ab", 10, new BigDecimal("100"))),
                Arguments.of(new CreateProductRequest("Valid Name", null, new BigDecimal("100"))),
                Arguments.of(new CreateProductRequest("Valid Name", 0, new BigDecimal("100"))),
                Arguments.of(new CreateProductRequest("Valid Name", 101, new BigDecimal("100"))),
                Arguments.of(new CreateProductRequest("Valid Name", 10, null)),
                Arguments.of(new CreateProductRequest("Valid Name", 10, new BigDecimal("0.9"))),
                Arguments.of(new CreateProductRequest("Valid Name", 10, new BigDecimal("50000.1")))
        );
    }

    @Test
    void createProduct_DatabaseError_ReturnsInternalServerError() throws Exception {
        // Arrange
        CreateProductRequest request =
                new CreateProductRequest("Valid Product", 10, new BigDecimal("100"));

        when(productService.createProduct(any(CreateProductRequest.class)))
                .thenThrow(mock(DataAccessException.class));

        // Act & Assert
        mockMvc.perform(post("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isInternalServerError(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Internal server database error")
                );

        verify(productService, times(1)).createProduct(any(CreateProductRequest.class));
    }

    @Test
    void createProduct_ValidData_ReturnsCreated() throws Exception {
        // Arrange
        CreateProductRequest request =
                new CreateProductRequest("Valid Product", 10, new BigDecimal("100"));

        when(productService.createProduct(any(CreateProductRequest.class))).thenReturn(UUID.randomUUID());

        // Act & Assert
        mockMvc.perform(post("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isCreated(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.productId").exists()
                );

        verify(productService, times(1)).createProduct(any(CreateProductRequest.class));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidPatchRequests")
    void patchProduct_InvalidData_ReturnsBadRequest(PatchProductRequest invalidRequest) throws Exception {
        // Arrange & Act & Assert
        mockMvc.perform(patch("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(invalidRequest)))
                .andExpectAll(
                        status().isBadRequest(),
                        content().contentType(MediaType.APPLICATION_JSON)
                );

        verifyNoInteractions(productService);
    }

    private static Stream<Arguments> provideInvalidPatchRequests() {
        UUID id = UUID.randomUUID();
        return Stream.of(
                Arguments.of(new PatchProductRequest(null, 10, new BigDecimal("100"))),
                Arguments.of(new PatchProductRequest(id, 0, new BigDecimal("100"))),
                Arguments.of(new PatchProductRequest(id, 101, new BigDecimal("100"))),
                Arguments.of(new PatchProductRequest(id, 10, new BigDecimal("0.9"))),
                Arguments.of(new PatchProductRequest(id, 10, new BigDecimal("50000.1")))
        );
    }

    @Test
    void patchProduct_ProductNotFound_ReturnsNotFound() throws Exception {
        // Arrange
        UUID productId = UUID.randomUUID();
        PatchProductRequest request = new PatchProductRequest(productId, 10, new BigDecimal("100"));

        when(productService.patchProduct(any(PatchProductRequest.class)))
                .thenThrow(new ProductNotFoundException(productId));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isNotFound(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Product with id " + productId + " not found")
                );

        verify(productService, times(1)).patchProduct(any(PatchProductRequest.class));
    }

    @Test
    void patchProduct_ProductOverflow_ReturnsConflict() throws Exception {
        // Arrange
        PatchProductRequest request =
                new PatchProductRequest(UUID.randomUUID(), 10, new BigDecimal("100"));

        when(productService.patchProduct(any(PatchProductRequest.class)))
                .thenThrow(new ProductOverflowException(150));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isConflict(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("The total quantity (150) exceeds the maximum allowed value (100) per item of the product")
                );

        verify(productService, times(1)).patchProduct(any(PatchProductRequest.class));
    }

    @Test
    void patchProduct_OptimisticLockingFailure_ReturnsConflict() throws Exception {
        // Arrange
        PatchProductRequest request =
                new PatchProductRequest(UUID.randomUUID(), 10, new BigDecimal("100"));

        when(productService.patchProduct(any(PatchProductRequest.class)))
                .thenThrow(mock(OptimisticLockingFailureException.class));

        // Act & Assert
        mockMvc.perform(patch("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isConflict(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.error").value("Data was modified by another user. Please refresh.")
                );

        verify(productService, times(1)).patchProduct(any(PatchProductRequest.class));
    }

    @Test
    void patchProduct_ValidData_ReturnsUpdatedProduct() throws Exception {
        // Arrange
        UUID productId = UUID.randomUUID();
        PatchProductRequest request =
                new PatchProductRequest(productId, 10, new BigDecimal("100"));
        ProductData response =
                new ProductData(productId, "Product Name", 10, new BigDecimal("100"));

        when(productService.patchProduct(any(PatchProductRequest.class))).thenReturn(response);

        // Act & Assert
        mockMvc.perform(patch("/api/v1/product")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpectAll(
                        status().isOk(),
                        content().contentType(MediaType.APPLICATION_JSON),
                        jsonPath("$.id").value(productId.toString()),
                        jsonPath("$.name").value("Product Name"),
                        jsonPath("$.quantity").value(10),
                        jsonPath("$.price").value(100)
                );

        verify(productService, times(1)).patchProduct(any(PatchProductRequest.class));
    }
}
