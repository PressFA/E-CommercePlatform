--liquibase formatted sql

--changeset pressf:1
CREATE TABLE processed_messages (
    id BIGINT,
    message_id VARCHAR(40) NOT NULL,

    CONSTRAINT PK_ProcessedMessages PRIMARY KEY (id),
    CONSTRAINT UQ_ProcessedMessages_message_id UNIQUE (message_id)
);
--rollback DROP TABLE IF EXISTS processed_messages;