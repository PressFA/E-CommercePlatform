--liquibase formatted sql

--changeset pressf:1
CREATE TABLE orders (
    id UUID,
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(12) NOT NULL,

    CONSTRAINT PK_Orders PRIMARY KEY (id)
);
--rollback DROP TABLE IF EXISTS orders;

--changeset pressf:2
CREATE TABLE orders_history (
    id UUID,
    order_id UUID NOT NULL,
    status VARCHAR(12) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    reason VARCHAR(255) NOT NULL,

    CONSTRAINT PK_OrdersHistory PRIMARY KEY (id),
    CONSTRAINT FK_OrdersHistory_order_id FOREIGN KEY (order_id) REFERENCES orders(id)
);
--rollback DROP TABLE IF EXISTS orders_history;

--changeset pressf:3
CREATE TABLE processed_messages (
    id BIGINT,
    message_id VARCHAR(40) NOT NULL,

    CONSTRAINT PK_ProcessedMessages PRIMARY KEY (id),
    CONSTRAINT UQ_ProcessedMessages_message_id UNIQUE (message_id)
);
--rollback DROP TABLE IF EXISTS processed_messages;