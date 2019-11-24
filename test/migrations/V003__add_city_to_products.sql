ALTER TABLE products
  ADD COLUMN city_id integer REFERENCES cities(id);
