--liquibase formatted sql

--changeset pressf:1
CREATE TABLE shopping_carts (
    id UUID,
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INT NOT NULL,

    CONSTRAINT PK_ShoppingCarts PRIMARY KEY (id)
);