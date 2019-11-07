{:dependencies #{"V005__add_city_to_products.sql"}}
CREATE OR REPLACE VIEW products_in_cities AS (
  SELECT
    p.id,
    p.name AS product_name,
    c.name AS city_name
  FROM products p
  INNER JOIN cities c ON c.id = p.city_id
);
