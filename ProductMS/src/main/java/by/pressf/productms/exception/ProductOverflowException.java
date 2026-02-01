package by.pressf.productms.exception;

public class ProductOverflowException extends RuntimeException {
    public ProductOverflowException(Integer totalQuantity) {
        super("The total quantity (" + totalQuantity + ") exceeds the maximum allowed value (100) per item of the product");
    }
}
