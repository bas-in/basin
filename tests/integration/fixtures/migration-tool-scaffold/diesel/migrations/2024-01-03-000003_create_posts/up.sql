-- Diesel migration 3 up: Create posts table with JSONB metadata column
CREATE TABLE posts (
    id         SERIAL    PRIMARY KEY,
    author_id  INTEGER   NOT NULL,
    title      TEXT      NOT NULL,
    body       TEXT,
    metadata   JSONB,
    published  BOOLEAN   NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
