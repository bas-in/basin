-- Diesel migration 4 up: Add display_name to users
ALTER TABLE users ADD COLUMN display_name TEXT;
