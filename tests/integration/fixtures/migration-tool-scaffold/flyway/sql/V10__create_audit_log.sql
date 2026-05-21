-- V10: Create audit_log table for change tracking
CREATE TABLE audit_log (
    id         SERIAL PRIMARY KEY,
    table_name TEXT      NOT NULL,
    row_id     INTEGER   NOT NULL,
    action     TEXT      NOT NULL,
    changed_at TIMESTAMP NOT NULL DEFAULT now()
);
