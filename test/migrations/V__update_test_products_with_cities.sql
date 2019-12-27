/*
{:dependencies #{"V__add_city_to_products.sql"}}
*/
UPDATE products
SET
  city_id = 1
WHERE
  id = 1;

UPDATE products
SET
  city_id = 2
WHERE
  id = 2;
