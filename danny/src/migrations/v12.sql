BEGIN TRANSACTION;

ALTER TABLE result DROP COLUMN datastructures_bytes;

CREATE TABLE datastructures_bytes (
    id    INTEGER NOT NULL,
    hostname   TEXT,
    datastructures_bytes   INTEGER,
    FOREIGN KEY (id) REFERENCES result (id)
);

PRAGMA user_version = 13;

END TRANSACTION;