-- 000001 up: Create users table
CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    email      TEXT      NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
