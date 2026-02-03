package by.pressf.paymentms.dto;

public record StripeRefundDto(String idempotencyKey,
                              String stripeId) {
}
