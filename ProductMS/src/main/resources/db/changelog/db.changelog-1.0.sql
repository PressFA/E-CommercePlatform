--liquibase formatted sql

--changeset pressf:1
CREATE TABLE products (
    id UUID,
    name VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL NOT NULL,
    version INT NOT NULL DEFAULT 0,

    CONSTRAINT PK_Products PRIMARY KEY (id),
    CONSTRAINT UQ_Products_name UNIQUE (name)
);
--rollback DROP TABLE IF EXISTS products;

--changeset pressf:2
CREATE TABLE product_history (
    id UUID,
    order_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    status VARCHAR(12) NOT NULL,

    CONSTRAINT PK_ProductHistory PRIMARY KEY (id),
    CONSTRAINT FK_ProductHistory_product_id FOREIGN KEY (product_id) REFERENCES products(id)
);
--rollback DROP TABLE IF EXISTS product_history;

--changeset pressf:3
CREATE TABLE processed_messages (
    id BIGINT,
    message_id VARCHAR(40) NOT NULL,

    CONSTRAINT PK_ProcessedMessages PRIMARY KEY (id),
    CONSTRAINT UQ_ProcessedMessages_message_id UNIQUE (message_id)
);
--rollback DROP TABLE IF EXISTS processed_messages;