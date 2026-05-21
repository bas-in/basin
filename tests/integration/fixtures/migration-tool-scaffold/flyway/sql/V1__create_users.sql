-- V1: Create users table
CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    email      TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMP   NOT NULL DEFAULT now()
);
