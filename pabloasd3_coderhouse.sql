CREATE TABLE IF NOT EXISTS pabloasd3_coderhouse.albums (
    id INT,
    title VARCHAR(100),
    userId INT,
    CONSTRAINT pk_albums PRIMARY KEY (id)
) DISTSTYLE KEY
  DISTKEY (id)
  SORTKEY (title);