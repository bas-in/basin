-- sqlx migration 3: Create posts table
CREATE TABLE posts (
    id         SERIAL    PRIMARY KEY,
    author_id  INTEGER   NOT NULL,
    title      TEXT      NOT NULL,
    body       TEXT,
    published  BOOLEAN   NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
