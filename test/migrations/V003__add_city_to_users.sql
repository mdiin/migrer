ALTER TABLE users
  ADD COLUMN city_id integer NOT NULL REFERENCES cities(id);
