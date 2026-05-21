-- sqlx migration 1: Create users table
-- Reversible: no (sqlx up-only migrations by default)
CREATE TABLE users (
    id         SERIAL    PRIMARY KEY,
    email      TEXT      NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
