-- Diesel migration 4 down: Drop display_name from users
ALTER TABLE users DROP COLUMN IF EXISTS display_name;
