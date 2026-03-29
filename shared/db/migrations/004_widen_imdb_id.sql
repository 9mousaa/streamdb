-- Widen imdb_id to support episode keys like tt1234567:12:125
ALTER TABLE content ALTER COLUMN imdb_id TYPE varchar(30);
