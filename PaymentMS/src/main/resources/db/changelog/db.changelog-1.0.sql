--liquibase formatted sql

--changeset pressf:1
CREATE TABLE payments (
    id UUID,
    user_id UUID NOT NULL,
    order_id UUID,
    stripe_id VARCHAR(48) NOT NULL,
    amount DECIMAL NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    type VARCHAR(12) NOT NULL,

    CONSTRAINT PK_Payments PRIMARY KEY (id)
);
--rollback DROP TABLE IF EXISTS payments;

--changeset pressf:2
CREATE TABLE processed_messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    message_id VARCHAR(40) NOT NULL,

    CONSTRAINT PK_ProcessedMessages PRIMARY KEY (id),
    CONSTRAINT UQ_ProcessedMessages_message_id UNIQUE (message_id)
);
--rollback DROP TABLE IF EXISTS processed_messages;
