/*
{:dependencies #{"V__create_products_table.sql" "V__create_cities_table.sql"}}
*/
ALTER TABLE products
  ADD COLUMN city_id integer REFERENCES cities(id);
