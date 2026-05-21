-- V7: Create comments table
CREATE TABLE comments (
    id         SERIAL PRIMARY KEY,
    post_id    INTEGER     NOT NULL,
    author_id  INTEGER     NOT NULL,
    body       TEXT        NOT NULL,
    created_at TIMESTAMP   NOT NULL DEFAULT now()
);
