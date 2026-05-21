-- sqlx migration 4: Add display_name to users
ALTER TABLE users ADD COLUMN display_name TEXT;
