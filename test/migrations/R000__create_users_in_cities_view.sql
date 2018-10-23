CREATE OR REPLACE VIEW users_in_cities AS (
  SELECT
    u.id,
    u.name AS user_name,
    c.name AS city_name
  FROM users u
  INNER JOIN cities c ON c.id = u.city_id
);
