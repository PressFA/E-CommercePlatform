--liquibase formatted sql

--changeset pressf:1
CREATE TABLE users (
    id UUID DEFAULT gen_random_uuid(),
    username VARCHAR(24) NOT NULL,
    password VARCHAR(16) NOT NULL,
    name VARCHAR(16) NOT NULL,
    balance DECIMAL NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,

    CONSTRAINT PK_Users PRIMARY KEY (id),
    CONSTRAINT UQ_Users_username UNIQUE (username)
);
--rollback DROP TABLE IF EXISTS users;

--changeset pressf:2
CREATE TABLE processed_messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    message_id VARCHAR(40) NOT NULL,

    CONSTRAINT PK_ProcessedMessages PRIMARY KEY (id),
    CONSTRAINT UQ_ProcessedMessages_message_id UNIQUE (message_id)
);
--rollback DROP TABLE IF EXISTS processed_messages;