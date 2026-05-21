-- Diesel migration 2 up: Create teams table
CREATE TABLE teams (
    id         SERIAL PRIMARY KEY,
    name       TEXT      NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
