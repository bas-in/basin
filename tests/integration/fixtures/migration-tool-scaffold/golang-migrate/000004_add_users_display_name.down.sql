-- 000004 down: Remove display_name column from users
ALTER TABLE users DROP COLUMN IF EXISTS display_name;
